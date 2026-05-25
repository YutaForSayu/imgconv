package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

var (
	proxyCache = map[string]string{}
	proxyMu    sync.RWMutex
)

const (
	apiURL    = "https://api.freeconvert.com/v1/process/jobs"
	chunkSize = 5242880
	userAgent = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36"
	timeout   = 180 * time.Second
)

var supportedOutput = []string{
	"jpg", "jpeg", "png", "webp", "avif", "gif", "bmp", "tiff", "tif", "ico", "svg", "pdf", "heic", "heif",
}

var formatAlias = map[string]string{
	"jpg": "jpeg",
	"tif": "tiff",
}

type ConvertRequest struct {
	To      string `form:"to"`
	Quality int    `form:"quality"`
}

type ConvertResponse struct {
	Status    bool        `json:"Status"`
	Code      int         `json:"Code"`
	Input     string      `json:"Input"`
	ConvertTo string      `json:"Convert_to"`
	Result    interface{} `json:"Result"`
	Error     string      `json:"Error,omitempty"`
}

type FileResult struct {
	URL         string `json:"Url"`
	ProxyURL    string `json:"ProxyUrl,omitempty"`
	Path        string `json:"Path,omitempty"`
	Size        int64  `json:"Size,omitempty"`
	ContentType string `json:"ContentType,omitempty"`
}

type ImageConverterResponse struct {
	Service string `json:"service"`
	Version string `json:"version"`
	Routes  Routes `json:"routes"`
}

type Routes struct {
	Convert   RouteDetail `json:"/convert"`
	Formats   RouteDetail `json:"/formats"`
	TaskProxy  RouteDetail `json:"/task/:taskId/imgs/:file"`
}

type RouteDetail struct {
	Method string `json:"method"`
	Desc   string `json:"desc"`
}

func normalizeFormat(format string) string {
	v := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(format), "."))
	if alias, ok := formatAlias[v]; ok {
		return alias
	}
	return v
}

func outputExt(format string) string {
	v := normalizeFormat(format)
	if v == "jpeg" {
		return "jpg"
	}
	return v
}

func cleanName(name string) string {
	var result strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}

func sleep(d time.Duration) {
	time.Sleep(d)
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func randomInt(min, max int) int {
	b := make([]byte, 2)
	rand.Read(b)
	n := int(b[0])<<8 | int(b[1])
	return min + (n % (max - min))
}

func mimeType(filePath string) string {
	ext := filepath.Ext(filePath)
	t := mime.TypeByExtension(ext)
	if t == "" {
		return "application/octet-stream"
	}
	return t
}

func normalizeServer(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	port := u.Port()
	newHost := host
	if strings.HasPrefix(host, "server") {
		parts := strings.SplitN(host, "-", 2)
		if len(parts) == 2 {
			num := strings.TrimPrefix(parts[0], "server")
			newHost = "s" + num + "-" + parts[1]
		}
	}
	origin := u.Scheme + "://" + newHost
	if port != "" {
		origin += ":" + port
	}
	return origin, nil
}

func createJob(from, to string) (map[string]interface{}, error) {
	options := map[string]interface{}{
		"auto-orient": true,
		"strip":       true,
	}
	if contains([]string{"jpeg", "jpg", "webp", "avif", "heic", "heif"}, to) {
		options["quality"] = 100
	}
	if to == "jpeg" || to == "jpg" {
		options["background"] = "#FFFFFF"
	}

	payload := map[string]interface{}{
		"tasks": map[string]interface{}{
			"import": map[string]interface{}{
				"operation": "import/upload",
			},
			"convert": map[string]interface{}{
				"operation":     "convert",
				"input":         "import",
				"input_format":  from,
				"output_format": to,
				"options":       options,
			},
			"export-url": map[string]interface{}{
				"operation": "export/url",
				"input":     "convert",
			},
		},
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", apiURL, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer null")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://www.freeconvert.com")
	req.Header.Set("Referer", fmt.Sprintf("https://www.freeconvert.com/%s-to-%s/download", from, to))
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gagal membuat job: HTTP %d %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func uploadChunk(server, importTaskID, identifier, filename, mtype string, fileSize int64, chunk []byte, chunkNumber, totalChunks int) error {
	currentSize := len(chunk)

	q := url.Values{}
	q.Set("resumableChunkNumber", strconv.Itoa(chunkNumber))
	q.Set("resumableChunkSize", strconv.Itoa(chunkSize))
	q.Set("resumableCurrentChunkSize", strconv.Itoa(currentSize))
	q.Set("resumableTotalSize", strconv.FormatInt(fileSize, 10))
	q.Set("resumableType", mtype)
	q.Set("resumableIdentifier", identifier)
	q.Set("resumableFilename", filename)
	q.Set("resumableRelativePath", filename)
	q.Set("resumableTotalChunks", strconv.Itoa(totalChunks))

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("resumableChunkNumber", strconv.Itoa(chunkNumber))
	w.WriteField("resumableChunkSize", strconv.Itoa(chunkSize))
	w.WriteField("resumableCurrentChunkSize", strconv.Itoa(currentSize))
	w.WriteField("resumableTotalSize", strconv.FormatInt(fileSize, 10))
	w.WriteField("resumableType", mtype)
	w.WriteField("resumableIdentifier", identifier)
	w.WriteField("resumableFilename", filename)
	w.WriteField("resumableRelativePath", filename)
	w.WriteField("resumableTotalChunks", strconv.Itoa(totalChunks))
	fw, _ := w.CreateFormFile("file", filename)
	fw.Write(chunk)
	w.Close()

	uploadURL := fmt.Sprintf("%s/api/resumable/%s?%s", server, importTaskID, q.Encode())
	req, _ := http.NewRequest("POST", uploadURL, &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", "https://www.freeconvert.com")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload chunk %d gagal: HTTP %d %s", chunkNumber, resp.StatusCode, string(b))
	}
	return nil
}

func joinChunks(server, importTaskID, identifier string, fileSize int64) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("identifier", identifier)
	w.WriteField("fileSize", strconv.FormatInt(fileSize, 10))
	w.Close()

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/resumable/join/%s", server, importTaskID), &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Origin", "https://www.freeconvert.com")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join chunk gagal: HTTP %d %s", resp.StatusCode, string(b))
	}
	return nil
}

func finishUpload(server, importTaskID, identifier, filename, signature string) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("identifier", identifier)
	w.WriteField("fileName", filename)
	w.WriteField("signature", signature)
	w.Close()

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/upload/%s", server, importTaskID), &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Origin", "https://www.freeconvert.com")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("finalize upload gagal: HTTP %d %s", resp.StatusCode, string(b))
	}
	return nil
}

func readJob(selfURL string) (map[string]interface{}, error) {
	req, _ := http.NewRequest("GET", selfURL, nil)
	req.Header.Set("Authorization", "Bearer null")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Origin", "https://www.freeconvert.com")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cek job gagal: HTTP %d %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func collectURLs(v interface{}, out *[]string) {
	switch val := v.(type) {
	case string:
		if strings.HasPrefix(val, "http://") || strings.HasPrefix(val, "https://") {
			*out = append(*out, val)
		}
	case []interface{}:
		for _, item := range val {
			collectURLs(item, out)
		}
	case map[string]interface{}:
		for _, item := range val {
			collectURLs(item, out)
		}
	}
}

func pickTask(job map[string]interface{}, name string) map[string]interface{} {
	tasks, ok := job["tasks"].([]interface{})
	if !ok {
		return nil
	}
	for _, t := range tasks {
		task, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		if task["name"] == name || task["operation"] == name {
			return task
		}
	}
	return nil
}

func pollJob(selfURL, server, exportTaskID, outputFilename string) (map[string]interface{}, string, error) {
	for i := 0; i < 90; i++ {
		job, err := readJob(selfURL)
		if err != nil {
			return nil, "", err
		}

		status, _ := job["status"].(string)

		var allURLs []string
		collectURLs(job, &allURLs)

		var directURL string
		for _, u := range allURLs {
			if strings.Contains(u, "/task/") || strings.Contains(u, "download") || strings.Contains(u, "export") {
				if strings.Contains(u, exportTaskID) {
					directURL = u
					break
				}
			}
		}
		if directURL == "" && len(allURLs) > 0 {
			directURL = allURLs[0]
		}

		if directURL != "" && status != "processing" && status != "created" {
			return job, directURL, nil
		}

		if status == "completed" || status == "done" || status == "finished" {
			return job, fmt.Sprintf("%s/task/%s/%s", server, exportTaskID, outputFilename), nil
		}

		if status == "failed" || status == "error" {
			b, _ := json.Marshal(job)
			return nil, "", fmt.Errorf("convert gagal: %s", string(b))
		}

		exportTask := pickTask(job, "export/url")
		if exportTask != nil {
			et := exportTask["status"]
			if et == "completed" || et == "done" {
				var exportURLs []string
				collectURLs(exportTask, &exportURLs)
				if len(exportURLs) > 0 {
					return job, exportURLs[0], nil
				}
			}
		}

		sleep(2 * time.Second)
	}

	return nil, fmt.Sprintf("%s/task/%s/%s", server, exportTaskID, outputFilename), nil
}

func downloadFile(fileURL string) ([]byte, string, error) {
	req, _ := http.NewRequest("GET", fileURL, nil)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("download gagal: HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	ct := resp.Header.Get("Content-Type")
	return data, ct, nil
}

func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func convertImage(fileData []byte, origName, to string) (FileResult, error) {
	from := normalizeFormat(strings.TrimPrefix(filepath.Ext(origName), "."))
	to = normalizeFormat(to)

	if from == "" {
		return FileResult{}, fmt.Errorf("format input tidak bisa dibaca dari ekstensi file")
	}
	if to == "" {
		return FileResult{}, fmt.Errorf("parameter 'to' kosong")
	}

	normalized := make([]string, len(supportedOutput))
	for i, s := range supportedOutput {
		normalized[i] = normalizeFormat(s)
	}
	if !contains(normalized, to) {
		return FileResult{}, fmt.Errorf("format output tidak didukung: %s. Pilihan: %s", to, strings.Join(normalized, ", "))
	}
	if from == to {
		return FileResult{}, fmt.Errorf("from dan to sama-sama %s", to)
	}

	filename := cleanName(origName)
	basename := cleanName(strings.TrimSuffix(origName, filepath.Ext(origName)))
	ext := outputExt(to)
	outputFilename := fmt.Sprintf("%s.%s", basename, ext)
	fileSize := int64(len(fileData))
	mtype := mimeType(origName)
	identifier := fmt.Sprintf("%d-%d-%s-%s%s.%s",
		fileSize,
		randomInt(1000, 9999),
		basename,
		randomHex(8),
		from,
		strings.TrimPrefix(filepath.Ext(origName), "."),
	)

	job, err := createJob(from, to)
	if err != nil {
		return FileResult{}, fmt.Errorf("createJob: %w", err)
	}

	importTask := pickTask(job, "import/upload")
	exportTask := pickTask(job, "export/url")

	if importTask == nil {
		return FileResult{}, fmt.Errorf("import task tidak ditemukan")
	}
	if exportTask == nil {
		return FileResult{}, fmt.Errorf("export task tidak ditemukan")
	}

	result, _ := importTask["result"].(map[string]interface{})
	form, _ := result["form"].(map[string]interface{})
	formURL, _ := form["url"].(string)
	params, _ := form["parameters"].(map[string]interface{})
	signature, _ := params["signature"].(string)

	if formURL == "" || signature == "" {
		return FileResult{}, fmt.Errorf("form upload tidak ditemukan")
	}

	importTaskID, _ := importTask["id"].(string)
	exportTaskID, _ := exportTask["id"].(string)

	server, err := normalizeServer(formURL)
	if err != nil {
		return FileResult{}, fmt.Errorf("normalizeServer: %w", err)
	}

	totalChunks := int((fileSize + chunkSize - 1) / chunkSize)
	for i := 0; i < totalChunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize
		if end > fileSize {
			end = fileSize
		}
		chunk := fileData[start:end]
		if err := uploadChunk(server, importTaskID, identifier, filename, mtype, fileSize, chunk, i+1, totalChunks); err != nil {
			return FileResult{}, fmt.Errorf("uploadChunk: %w", err)
		}
	}

	if err := joinChunks(server, importTaskID, identifier, fileSize); err != nil {
		return FileResult{}, fmt.Errorf("joinChunks: %w", err)
	}
	if err := finishUpload(server, importTaskID, identifier, filename, signature); err != nil {
		return FileResult{}, fmt.Errorf("finishUpload: %w", err)
	}

	links, _ := job["links"].(map[string]interface{})
	selfURL, _ := links["self"].(string)

	_, dlURL, err := pollJob(selfURL, server, exportTaskID, outputFilename)
	if err != nil {
		return FileResult{}, fmt.Errorf("pollJob: %w", err)
	}

	proxyMu.Lock()
	proxyCache[exportTaskID] = dlURL
	proxyMu.Unlock()

	proxyURL := fmt.Sprintf("/task/%s/imgs/%s", exportTaskID, outputFilename)

	return FileResult{
		URL:      dlURL,
		ProxyURL: proxyURL,
	}, nil
}

func handleConvert(c *fiber.Ctx) error {
	to := c.FormValue("to")
	if to == "" {
		return c.Status(400).JSON(ConvertResponse{
			Status: false, Code: 400,
			Error: "Parameter 'to' wajib diisi",
		})
	}

	file, err := c.FormFile("file")
	if err != nil {
		return c.Status(400).JSON(ConvertResponse{
			Status: false, Code: 400,
			Error: "File wajib diupload (field: 'file')",
		})
	}

	f, err := file.Open()
	if err != nil {
		return c.Status(500).JSON(ConvertResponse{
			Status: false, Code: 500,
			Error: "Gagal membaca file",
		})
	}
	defer f.Close()

	fileData, err := io.ReadAll(f)
	if err != nil {
		return c.Status(500).JSON(ConvertResponse{
			Status: false, Code: 500,
			Error: "Gagal membaca data file",
		})
	}

	result, err := convertImage(fileData, file.Filename, to)
	if err != nil {
		return c.Status(500).JSON(ConvertResponse{
			Status:    false,
			Code:      500,
			Input:     file.Filename,
			ConvertTo: normalizeFormat(to),
			Result:    nil,
			Error:     err.Error(),
		})
	}

	return c.JSON(ConvertResponse{
		Status:    true,
		Code:      200,
		Input:     file.Filename,
		ConvertTo: normalizeFormat(to),
		Result:    result,
	})
}

// handleProxy proxies image from freeconvert CDN
// Route: GET /:taskId/imgs/:filename
// Builds: https://server*-*.freeconvert.com/task/:taskId/:filename
func handleProxy(c *fiber.Ctx) error {
	taskID := c.Params("taskId")
	filename := c.Params("filename")

	if taskID == "" || filename == "" {
		return c.Status(400).JSON(fiber.Map{
			"Status": false,
			"Code":   400,
			"Error":  "taskId dan filename wajib ada",
		})
	}

	// Task ID dari freeconvert punya prefix server di URL aslinya,
	// kita coba fetch langsung via CDN pattern mereka.
	// Format URL asli: https://server{N}-{hash}.freeconvert.com/task/{taskId}/{file}
	// Karena kita tidak tahu server-nya, simpan mapping taskId -> URL di memory.
	proxyMu.RLock()
	origin, ok := proxyCache[taskID]
	proxyMu.RUnlock()

	var targetURL string
	if ok {
		// Ganti filename saja (bisa jadi beda dari yang di-cache)
		parsed, err := url.Parse(origin)
		if err == nil {
			parts := strings.Split(parsed.Path, "/")
			// path: /task/{taskId}/{file}
			if len(parts) >= 3 {
				parts[len(parts)-1] = filename
				parsed.Path = strings.Join(parts, "/")
				targetURL = parsed.String()
			}
		}
	}

	if targetURL == "" {
		// Fallback: coba tebak via API task
		targetURL = fmt.Sprintf("https://www.freeconvert.com/task/%s/%s", taskID, filename)
	}

	req, _ := http.NewRequest("GET", targetURL, nil)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Referer", "https://www.freeconvert.com/")
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return c.Status(502).JSON(fiber.Map{
			"Status": false,
			"Code":   502,
			"Error":  "Gagal fetch file dari upstream: " + err.Error(),
		})
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return c.Status(resp.StatusCode).JSON(fiber.Map{
			"Status": false,
			"Code":   resp.StatusCode,
			"Error":  fmt.Sprintf("Upstream mengembalikan HTTP %d", resp.StatusCode),
		})
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}

	c.Set("Content-Type", ct)
	c.Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, filename))
	c.Set("Cache-Control", "public, max-age=86400")

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"Status": false,
			"Code":   500,
			"Error":  "Gagal membaca response upstream",
		})
	}

	return c.Status(200).Send(data)
}

func handleFormats(c *fiber.Ctx) error {
	normalized := make([]string, 0)
	seen := map[string]bool{}
	for _, s := range supportedOutput {
		n := normalizeFormat(s)
		if !seen[n] {
			normalized = append(normalized, n)
			seen[n] = true
		}
	}
	return c.JSON(fiber.Map{
		"supported_formats": normalized,
	})
}

func main() {
	app := fiber.New(fiber.Config{
		BodyLimit:    50 * 1024 * 1024,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	})

	app.Use(logger.New(logger.Config{
		Format: "[${time}] ${status} ${method} ${path} - ${latency}\n",
	}))
	
	app.Use(cors.New(cors.Config{
		AllowOrigins: strings.Join([]string{
			"https://kcast.bani.biz.id",
			"https://kcast.nvs.my.id",
			"http://localhost:8080",
		}, ","),
		AllowCredentials: true,
		AllowMethods: "GET,POST,OPTIONS",
		AllowHeaders: strings.Join([]string{
			"Origin",
			"Content-Type",
			"Accept",
			"Authorization",
		}, ","),
	}))

	app.Use(limiter.New(limiter.Config{
		Max:        25,
		Expiration: 15 * time.Second,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(429).JSON(fiber.Map{
				"Status": false,
				"Code":   429,
				"Error":  "Rate limit tercapai. Maksimal 25 request per 15 detik.",
			})
		},
	}))

	app.Get("/", func(c *fiber.Ctx) error {
		response := ImageConverterResponse{
			Service: "Image Converter API",
			Version: "1.0.0",
			Routes: Routes{
				Convert: RouteDetail{
					Method: "POST",
					Desc:   "Convert gambar (form-data: file, to)",
				},
				Formats: RouteDetail{
					Method: "GET",
					Desc:   "Daftar format yang didukung",
				},
				TaskProxy: RouteDetail{
					Method: "GET",
					Desc:   "Proxy gambar hasil convert",
				},
			},
		}
		return c.JSON(response)
	})

	app.Post("/convert", handleConvert)
	app.Get("/formats", handleFormats)
	app.Get("/task/:taskId/imgs/:filename", handleProxy)

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	log.Printf("Server jalan di :%s", port)
	log.Fatal(app.Listen(":" + port))
}

