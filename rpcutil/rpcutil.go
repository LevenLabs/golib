// Package rpcutil provides various methods for working with gorilla's JSON RPC
// 2 interface (http://www.gorillatoolkit.org/pkg/rpc/v2/json2)
package rpcutil

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"gopkg.in/validator.v2"

	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/levenlabs/go-applicator"
	"github.com/levenlabs/go-llog"
)

func init() {
	gob.Register(&brwMarshalled{})
}

// A QuietError is a json2.Error except that it will not cause a warn log when
// returned from a handler
type QuietError json2.Error

func (e *QuietError) Error() string {
	return e.Message
}

// RequestKV returns a basic KV for passing into llog, filled with entries
// related to the passed in http.Request
func RequestKV(r *http.Request) llog.KV {
	kv := llog.KV{
		"ip": RequestIP(r),
	}
	// first try Referer, but fallback to Origin
	if ref := r.Header.Get("Referer"); ref != "" {
		kv["referer"] = ref
	} else if o := r.Header.Get("Origin"); o != "" {
		kv["origin"] = o
	}
	if via := r.Header.Get("Via"); via != "" {
		kv["via"] = via
	}
	return kv
}

// we don't ever really pass this into encoding/json, so having it implement
// json.Marshaler isn't really necessary, but it's helpful to think of it in
// this way
type jsonInliner struct {
	orig  interface{}
	extra map[string]interface{}
}

func (j jsonInliner) MarshalJSON() ([]byte, error) {
	bOrig, err := json.Marshal(j.orig)
	if err != nil {
		return nil, err
	}
	if len(bOrig) < 2 || bOrig[len(bOrig)-1] != '}' {
		return nil, errors.New("jsonInliner original value not an object")
	}
	if len(j.extra) == 0 {
		return bOrig, nil
	}

	bExtra, err := json.Marshal(j.extra)
	if err != nil {
		return nil, err
	}

	bOrig = bOrig[:len(bOrig)-1]
	bOrig = append(bOrig, ',')
	bOrig = append(bOrig, bExtra[1:]...)
	return bOrig, nil
}

// LLCodec wraps around gorilla's json2.Codec, adding logging to all requests
type LLCodec struct {
	c rpc.Codec

	// If true any errors which are not user caused (error code < 1) will not
	// actually be returned to the client, only a generic error message in their
	// place
	HideServerErrors bool

	// If true the gopkg.in/validator.v2 package will be used to automatically
	// validate inputs to calls
	ValidateInput bool

	// If true the github.com/levenlabs/go-applicator package will be used to
	// apply functions to your input fields
	RunInputApplicators bool

	// If set, once a non-error response is returned by an rpc endpoint this
	// will be called and the result (if non-nil) will be inlined with the
	// original response. The original response must encode to a json object for
	// this to work.
	//
	// For example, if the original response encodes to `{"success":true}`, and
	// ResponseInliner returns `{"currentTime":123456}`, the final response sent
	// to the client will be `{"success":true,"currentTime":123456}`
	ResponseInliner func(*http.Request) map[string]interface{}

	// All endpoints (fullname, i.e. "Service.Method") set as keys in this map
	// will not have an INFO log printed out when they are hit
	ExcludeRequestLog map[string]bool
}

// NewLLCodec returns an LLCodec, which is an implementation of rpc.Codec around
// json2.Codec. All public fields on LLCodec can be modified up intil passing
// this into rpc.RegisterCodec
func NewLLCodec() LLCodec {
	return LLCodec{c: json2.NewCodec(), ExcludeRequestLog: map[string]bool{}}
}

// NewRequest implements the NewRequest method for the rpc.Codec interface
func (c LLCodec) NewRequest(r *http.Request) rpc.CodecRequest {
	return llCodecRequest{
		c:            &c,
		CodecRequest: c.c.NewRequest(r),
		r:            r,
		kv:           RequestKV(r),
	}
}

type llCodecRequest struct {
	c *LLCodec
	rpc.CodecRequest
	r  *http.Request
	kv llog.KV
}

func stringifyInterface(args interface{}) string {
	if rm, ok := args.(*json.RawMessage); ok {
		return string(*rm)
	}
	if by, ok := args.([]byte); ok {
		return string(by)
	}
	return fmt.Sprintf("%+v", args)
}

func (cr llCodecRequest) ReadRequest(args interface{}) error {
	// After calling the underlying ReadRequest the args will be filled in
	if err := cr.CodecRequest.ReadRequest(args); err != nil {
		// err will already be a json2.Error in this specific case, we don't
		// have to wrap it again
		return err
	}

	method, _ := cr.CodecRequest.Method()
	cr.kv["method"] = method
	var fn llog.LogFunc
	if cr.c.ExcludeRequestLog[method] {
		// don't log anything
	} else if llog.GetLevel() == llog.DebugLevel {
		cr.kv["args"] = stringifyInterface(args)
		cr.kv["argsType"] = fmt.Sprintf("%T", args)
		fn = llog.Debug
	} else {
		fn = llog.Info
	}

	if cr.c.RunInputApplicators {
		if err := applicator.Apply(args); err != nil {
			// presumably the error is an internal one, but leave the option
			// for the function to return a json2.Error if they want
			return err
		}
	}

	if cr.c.ValidateInput {
		if err := validator.Validate(args); err != nil {
			return &json2.Error{
				Code:    json2.E_BAD_PARAMS,
				Message: err.Error(),
			}
		}
	}

	if fn != nil {
		fn("jsonrpc incoming request", cr.kv)
	}

	return nil
}

func (cr llCodecRequest) maybeInlineExtra(r interface{}) (interface{}, error) {
	if cr.c.ResponseInliner == nil {
		return r, nil
	}
	extra := cr.c.ResponseInliner(cr.r)
	if extra == nil {
		return r, nil
	}

	j := jsonInliner{orig: r, extra: extra}
	b, err := j.MarshalJSON()
	if err != nil {
		return nil, err
	}
	jr := json.RawMessage(b)
	return &jr, nil
}

func (cr llCodecRequest) WriteResponse(w http.ResponseWriter, r interface{}) {
	llog.Debug("jsonrpc responding", cr.kv)
	newR, err := cr.maybeInlineExtra(r)
	if err != nil {
		kv := llog.KV{
			"err": err,
		}
		kv["orig"], _ = json.Marshal(r)
		llog.Error("jsonrpc could not inline extra", cr.kv, kv)
	} else {
		r = newR
	}

	cr.CodecRequest.WriteResponse(w, r)
}

func (cr llCodecRequest) WriteError(w http.ResponseWriter, status int, err error) {
	// status is ignored by gorilla

	cr.kv["err"] = err

	var quietErr *QuietError
	var jsonErr *json2.Error
	switch terr := err.(type) {
	case *QuietError:
		quietErr = terr
		jsonErr = (*json2.Error)(terr)
	case *json2.Error:
		jsonErr = terr
	default:
		jsonErr = &json2.Error{
			Code:    json2.E_SERVER,
			Message: fmt.Sprintf("unexpected internal server error: %s", err),
		}
	}
	if kv, ok := jsonErr.Data.(llog.KV); ok {
		for k, v := range kv {
			cr.kv[k] = v
		}
	}

	// The only predefined error that is considered a server error really is
	// E_SERVER, all the others which are less than it are basically client
	// errors. So all within this range are considered internal server errors,
	// and need to be possibly hidden and definitely output as errors
	if jsonErr.Code < 0 && jsonErr.Code >= json2.E_SERVER {
		if cr.c.HideServerErrors {
			jsonErr = &json2.Error{
				Code:    json2.E_SERVER,
				Message: "internal server error",
			}
		}
		llog.Error("jsonrpc internal server error", cr.kv)
	} else if quietErr == nil {
		llog.Warn("jsonrpc client error", cr.kv)
	}

	// its okay to always return jsonErr since quietErr is a jsonErr
	cr.CodecRequest.WriteError(w, status, jsonErr)
}
