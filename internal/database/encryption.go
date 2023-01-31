package database

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/minio/sio"
	"golang.org/x/crypto/hkdf"
)

// validateEncryptionKey returns an error if the key is not valid.
// A valid key must be 16, 24 or 32 bytes long.
func validateEncryptionKey(key []byte) error {
	switch len(key) {
	case 16, 24, 32:
		return nil
	default:
		return errors.New("invalid encryption key size")
	}
}

func NewEncryptedFS(fs vfs.FS, secret []byte) vfs.FS {
	return &encryptedFS{FS: fs, secret: secret}
}

// encryptedFS is an implementation of the vfs.FS interface that encrypts
// and decrypts data using AES-256.
type encryptedFS struct {
	vfs.FS
	secret []byte
}

func (fs *encryptedFS) getKey(iv []byte) (key [32]byte, err error) {
	kdf := hkdf.New(sha256.New, fs.secret, iv, nil)
	if _, err = io.ReadFull(kdf, key[:]); err != nil {
		return key, fmt.Errorf("failed to derive encryption key: %v", err)
	}
	return key, nil
}

func (fs *encryptedFS) Rename(oldname, newname string) error {
	err := fs.FS.Rename(oldname, newname)
	if err != nil {
		return err
	}
	// rename the IV file
	return fs.FS.Rename(oldname+".iv", newname+".iv")
}

func (fs *encryptedFS) Link(oldname, newname string) error {
	err := fs.FS.Link(oldname, newname)
	if err != nil {
		return err
	}

	// link the IV file
	return fs.FS.Link(oldname+".iv", newname+".iv")
}

func (fs *encryptedFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}

	// reuse the IV file
	ivf, err := fs.FS.Open(oldname + ".iv")
	if err != nil {
		return nil, err
	}
	defer ivf.Close()

	iv, err := io.ReadAll(ivf)
	if err != nil {
		return nil, err
	}

	ivf2, err := fs.FS.Create(newname + ".iv")
	if err != nil {
		return nil, err
	}
	defer ivf2.Close()

	_, err = ivf2.Write(iv)
	if err != nil {
		return nil, err
	}

	key, err := fs.getKey(iv)
	if err != nil {
		return nil, err
	}

	return &encryptedFile{
		file: f,
		key:  key,
	}, nil
}

func (fs *encryptedFS) Remove(name string) error {
	err := fs.FS.Remove(name)
	if err != nil {
		return err
	}

	// remove the IV file
	return fs.FS.Remove(name + ".iv")
}

func (fs *encryptedFS) RemoveAll(dir string) error {
	err := fs.FS.RemoveAll(dir)
	if err != nil {
		return err
	}

	// remove all IV files
	return fs.FS.RemoveAll(dir + ".iv")
}

func (fs *encryptedFS) List(dir string) ([]string, error) {
	list, err := fs.FS.List(dir)
	if err != nil {
		return nil, err
	}

	// remove the IV files from the list
	filtered := make([]string, 0, len(list))
	for _, name := range list {
		if filepath.Ext(name) != ".iv" {
			filtered = append(filtered, name)
		}
	}

	return filtered, nil
}

func (fs *encryptedFS) Stat(name string) (os.FileInfo, error) {
	fi, err := fs.FS.Stat(name)
	if err != nil {
		return nil, err
	}

	return &encryptedFileInfo{
		FileInfo: fi,
	}, nil
}

func (fs *encryptedFS) Create(name string) (vfs.File, error) {
	file, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}

	// new file, create a new IV
	var iv [32]byte
	if _, err := io.ReadFull(rand.Reader, iv[:]); err != nil {
		return nil, err
	}
	// create another file to write the IV to
	ivFile, err := fs.FS.Create(name + ".iv")
	if err != nil {
		return nil, err
	}
	defer ivFile.Close()

	// write the IV to the file
	if _, err := ivFile.Write(iv[:]); err != nil {
		return nil, err
	}

	// derive an encryption key from the master key and the iv
	key, err := fs.getKey(iv[:])
	if err != nil {
		return nil, err
	}

	return &encryptedFile{
		name: name,
		key:  key,
		file: file,
	}, nil
}

func (fs *encryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}

	// open the IV file
	ivFile, err := fs.FS.Open(name+".iv", opts...)
	if err != nil {
		// if the IV file does not exist, the file is not encrypted
		if os.IsNotExist(errors.UnwrapAll(err)) {
			return file, nil
		}

		return nil, err
	}
	defer ivFile.Close()

	// read the IV from the file
	var iv [32]byte
	if _, err := io.ReadFull(ivFile, iv[:]); err != nil {
		return nil, err
	}

	// derive an encryption key from the master key and the iv
	key, err := fs.getKey(iv[:])
	if err != nil {
		return nil, err
	}

	return &encryptedFile{
		name: name,
		key:  key,
		file: file,
	}, nil
}

type encryptedFile struct {
	name        string
	key         [32]byte
	reader      io.Reader
	readerAt    io.ReaderAt
	writeCloser io.WriteCloser
	file        vfs.File
}

func (e *encryptedFile) Read(p []byte) (n int, err error) {
	if e.reader == nil {
		var err error
		e.reader, err = sio.DecryptReader(e.file, sio.Config{Key: e.key[:]})
		if err != nil {
			return 0, err
		}
	}

	return e.reader.Read(p)
}

func (e *encryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	if e.readerAt == nil {
		var err error
		e.readerAt, err = sio.DecryptReaderAt(e.file, sio.Config{Key: e.key[:]})
		if err != nil {
			return 0, err
		}
	}

	return e.readerAt.ReadAt(p, off)
}

func (e *encryptedFile) Write(p []byte) (n int, err error) {
	if e.writeCloser == nil {
		e.writeCloser, err = sio.EncryptWriter(e.file, sio.Config{Key: e.key[:]})
		if err != nil {
			return 0, err
		}
	}

	return e.writeCloser.Write(p)
}

func (e *encryptedFile) Sync() error {
	return e.file.Sync()
}

func (e *encryptedFile) Close() error {
	if e.writeCloser != nil {
		err := e.writeCloser.Close()
		e.writeCloser = nil
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *encryptedFile) Stat() (os.FileInfo, error) {
	fi, err := e.file.Stat()
	if err != nil {
		return nil, err
	}

	return &encryptedFileInfo{fi}, nil
}

func (e *encryptedFile) Fd() uintptr {
	return e.file.Fd()
}

type encryptedFileInfo struct {
	os.FileInfo
}

func (fi *encryptedFileInfo) Size() int64 {
	size, _ := sio.DecryptedSize(uint64(fi.FileInfo.Size()))

	return int64(size)
}
