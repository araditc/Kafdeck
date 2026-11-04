// package builds a provider ZIP with portable executable permissions even when
// cross-compiling on Windows. Stable metadata makes identical binary ZIPs repeatable.
package main

import (
	"archive/zip"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: package INPUT_BINARY OUTPUT_ZIP")
	}
	if err := pack(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}
func pack(input, output string) error {
	src, e := os.Open(input)
	if e != nil {
		return e
	}
	defer src.Close()
	dst, e := os.Create(output)
	if e != nil {
		return e
	}
	archive := zip.NewWriter(dst)
	header := &zip.FileHeader{Name: filepath.Base(input), Method: zip.Deflate}
	header.SetMode(0755)
	header.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
	entry, e := archive.CreateHeader(header)
	if e == nil {
		_, e = io.Copy(entry, src)
	}
	if closeErr := archive.Close(); e == nil {
		e = closeErr
	}
	if closeErr := dst.Close(); e == nil {
		e = closeErr
	}
	return e
}
