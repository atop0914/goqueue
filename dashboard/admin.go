package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/atop0914/goqueue"
)

// ---- admin API (POST endpoints) ----

// Every admin endpoint is POST-only (state-changing operations must not be
// triggerable by prefetchers or accidental GETs) and answers with a small
// JSON envelope: {"ok":true, ...} or {"ok":false,"error":"..."} with an
// appropriate status code.

// pauseResponse is the /api/admin/pause and /api/admin/resume payload.
type pauseResponse struct {
	OK      bool `json:"ok"`
	Paused  bool `json:"paused"`
	Started bool `json:"started"`
}

// purgeResponse is the /api/admin/purge payload.
type purgeResponse struct {
	OK       bool `json:"ok"`
	Purged   int  `json:"purged"`
	WithDead bool `json:"with_dead"`
}

// requeueResponse is the /api/admin/requeue-dead payload.
type requeueResponse struct {
	OK       bool   `json:"ok"`
	Requeued int    `json:"requeued"`
	JobID    string `json:"job_id,omitempty"`
}

// errorResponse is the failure envelope.
type errorResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

func writeAdminError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(errorResponse{OK: false, Error: err.Error()})
}

func writeAdminJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

// methodNotAllowed answers 405 for admin endpoints hit with a method other
// than POST (e.g. a GET from a prefetcher). The Allow header advertises the
// supported method.
func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// POST fell through here only when the pattern-specific handler was
		// not registered (cannot happen with the current routes).
		writeAdminError(w, http.StatusNotFound, errors.New("not found"))
		return
	}
	w.Header().Set("Allow", http.MethodPost)
	writeAdminError(w, http.StatusMethodNotAllowed, errors.New("method "+r.Method+" not allowed"))
}

// requirePost rejects non-POST (and non-HEAD for symmetric mux behavior)
// requests with 405.
func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost {
		return true
	}
	w.Header().Set("Allow", http.MethodPost)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

// handlePause implements POST /api/admin/pause.
func (d *Dashboard) handlePause(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	if err := d.cli.Pause(); err != nil {
		writeAdminError(w, http.StatusConflict, err)
		return
	}
	writeAdminJSON(w, pauseResponse{OK: true, Paused: true, Started: d.cli.Stats().Started})
}

// handleResume implements POST /api/admin/resume.
func (d *Dashboard) handleResume(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	d.cli.Resume()
	writeAdminJSON(w, pauseResponse{OK: true, Paused: d.cli.IsPaused(), Started: d.cli.Stats().Started})
}

// handlePurge implements POST /api/admin/purge. The optional JSON body
// {"dead":true} also purges the dead-letter set; an empty body purges
// pending jobs only.
func (d *Dashboard) handlePurge(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	var body struct {
		Dead bool `json:"dead"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeAdminError(w, http.StatusBadRequest, err)
			return
		}
	}
	n, err := d.cli.Purge(r.Context(), body.Dead)
	if err != nil {
		writeAdminError(w, http.StatusNotImplemented, err)
		return
	}
	writeAdminJSON(w, purgeResponse{OK: true, Purged: n, WithDead: body.Dead})
}

// handleRequeueDead implements POST /api/admin/requeue-dead. Without a body
// (or with {"all":true}) every dead job is requeued; with {"id":"..."} only
// that job is requeued.
func (d *Dashboard) handleRequeueDead(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	var body struct {
		All bool   `json:"all"`
		ID  string `json:"id"`
	}
	hasBody := r.Body != nil && r.ContentLength != 0
	if hasBody {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeAdminError(w, http.StatusBadRequest, err)
			return
		}
	}
	if body.ID != "" {
		if err := d.cli.RequeueDeadJob(r.Context(), body.ID); err != nil {
			code := http.StatusInternalServerError
			switch err {
			case goqueue.ErrJobNotFound:
				code = http.StatusNotFound
			case goqueue.ErrJobExists:
				code = http.StatusConflict
			}
			writeAdminError(w, code, err)
			return
		}
		writeAdminJSON(w, requeueResponse{OK: true, Requeued: 1, JobID: body.ID})
		return
	}
	_ = hasBody // "all" and empty bodies behave identically: flush the DLQ
	n, err := d.cli.RequeueDead(r.Context())
	if err != nil {
		writeAdminError(w, http.StatusNotImplemented, err)
		return
	}
	writeAdminJSON(w, requeueResponse{OK: true, Requeued: n})
}
