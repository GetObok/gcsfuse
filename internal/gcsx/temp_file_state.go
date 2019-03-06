package gcsx

import (
	"fmt"

	"github.com/jacobsa/gcloud/gcs"

	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

const stateFileName = "status.json"

type tempFileStat struct {
	Name       string
	Synced     bool
	Generation int64
}

type TempFileState struct {
	cacheDir string
	syncer   Syncer

	mu    sync.Mutex
	state map[string]tempFileStat

	// initially unsynced temp files
	unsynced []string
}

func NewTempFileState(cacheDir string, syncer Syncer) (*TempFileState, error) {
	tfs := &TempFileState{
		cacheDir: cacheDir,
		syncer:   syncer,
		state:    make(map[string]tempFileStat),
	}

	if err := tfs.restore(); err != nil {
		return nil, fmt.Errorf("failed to restore %q: %v", tfs.stateFile(), err)
	}
	return tfs, nil
}

func (p *TempFileState) stateFile() string {
	return path.Join(p.cacheDir, stateFileName)
}

func (p *TempFileState) restore() error {
	bytes, err := ioutil.ReadFile(p.stateFile())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	if len(bytes) > 0 {
		if err := json.Unmarshal(bytes, &p.state); err != nil {
			return err
		}
		for t := range p.state {
			p.unsynced = append(p.unsynced, t)
		}
	}
	return nil
}

func (p *TempFileState) MarkForUpload(tmpFile, dstPath string, generation int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state[tmpFile] = tempFileStat{
		Name:       dstPath,
		Generation: generation,
	}
	return p.flush()
}

func (p *TempFileState) MarkUploaded(tmpFile string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.state[tmpFile]
	s.Synced = true
	p.state[tmpFile] = s
	return p.flush()
}

func (p *TempFileState) DeleteFileStatus(tmpFile string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.state, tmpFile)
	return p.flush()
}

func (p *TempFileState) UpdatePaths(oldPath, newPath string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for t, s := range p.state {
		if strings.HasPrefix(s.Name, oldPath) {
			p2 := strings.TrimPrefix(s.Name, oldPath)
			newName := path.Join(newPath, p2)
			if strings.HasSuffix(t, "/") {
				newName = newName + "/"
			}
			s.Name = newName
			p.state[t] = s
		}
	}
	return p.flush()
}

func (p *TempFileState) UploadUnsynced(ctx context.Context) {
	var unsynced []string
	for _, t := range p.unsynced {
		p.mu.Lock()
		f, stateExists := p.state[t]
		p.mu.Unlock()
		tfile, err := NewTempFileRO(t)
		if err != nil && !os.IsNotExist(err) {
			log.Println("local cache file sync: failed to open temp file.", t, err)
			unsynced = append(unsynced, t)
			continue
		} else if err == nil {
			if stateExists && !f.Synced {
				log.Println("local cache file sync.", t, f.Name)
				err := p.uploadTmpFile(ctx, tfile, f)
				if err == nil {
					log.Println("local cache file sync done.", t, f.Name)
				} else {
					log.Println("local cache file sync failed.", t, f.Name, err)
					if _, ok := err.(*gcs.PreconditionError); !ok {
						unsynced = append(unsynced, t)
						continue
					}
				}
			} else {
				log.Println("local cache file already synced.", t, f.Name)
			}
			tfile.Destroy()
		}

		p.DeleteFileStatus(t)
	}
	p.unsynced = unsynced
}

func (p *TempFileState) txFile() string {
	return path.Join(p.cacheDir, "."+stateFileName+".tx")
}

func (p *TempFileState) flush() error {
	data, err := json.Marshal(p.state)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(p.txFile(), data, 0600); err != nil {
		return err
	}

	return os.Rename(p.txFile(), p.stateFile())
}

func (p *TempFileState) uploadTmpFile(ctx context.Context, tfile TempFileRO, f tempFileStat) error {
	obj := gcs.Object{
		Name:       f.Name,
		Generation: f.Generation,
	}

	_, err := p.syncer.SyncObject(ctx, &obj, tfile)
	return err
}
