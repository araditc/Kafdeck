package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestCrossPlatformZipExtractsExecutable(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "terraform-provider-kafdeck_v0.8.0")
	output := filepath.Join(dir, "provider.zip")
	if e := os.WriteFile(input, []byte("binary fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := pack(input, output); e != nil {
		t.Fatal(e)
	}
	archive, e := zip.OpenReader(output)
	if e != nil {
		t.Fatal(e)
	}
	defer archive.Close()
	if len(archive.File) != 1 || archive.File[0].Mode().Perm() != 0755 {
		t.Fatal("offline Linux/macOS mirror would extract a nonexecutable provider")
	}
}
