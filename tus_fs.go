package TusFS

import (
	"errors"
	"github.com/bdragon300/tusgo"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

type Fs struct {
	client   *tusgo.Client
	u        string
	download string
}

func New(u string) (*Fs, error) {
	if strings.HasSuffix(u, "/") {
		panic("has suffix")
	}

	baseURL, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	cl := tusgo.NewClient(http.DefaultClient, baseURL)

	fs := &Fs{client: cl, u: u, download: strings.Replace(u, "/files", "/download", 1)}

	return fs, nil
}

func (fs *Fs) ReadFile(path string) (io.ReadCloser, error) {
	resp, err := http.Get(fs.download + filepath.Join("/", path))
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (fs *Fs) WriteFile(path string, src io.ReadSeeker, fileSize int64, attempts int) error {
	meta := make(map[string]string)
	meta["Path"] = path

	u := tusgo.Upload{}
	if _, err := fs.client.CreateUpload(&u, fileSize, false, meta); err != nil {
		return err
	}

	dst := tusgo.NewUploadStream(fs.client, &u)

	if _, err := dst.Sync(); err != nil {
		return err
	}
	if _, err := src.Seek(dst.Tell(), io.SeekStart); err != nil {
		return err
	}

	_, err := io.Copy(dst, src)
	a := attempts
	for err != nil && a > 0 {
		slog.Error(err.Error())
		if _, ok := err.(net.Error); !ok && !errors.Is(err, tusgo.ErrChecksumMismatch) {
			return err // Permanent error, no luck
		}
		time.Sleep(5 * time.Second)
		a--

		_, err = dst.Sync()
		_, err = src.Seek(dst.Tell(), io.SeekStart)
		_, err = io.Copy(dst, src)
	}

	if a == 0 {
		return errors.New("too many attempts to upload the data")
	}

	return nil

}
