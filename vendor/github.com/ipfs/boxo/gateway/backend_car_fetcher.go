package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/path"
)

type DataCallback func(p path.ImmutablePath, reader io.Reader) error

// CarFetcher powers a [CarBackend].
type CarFetcher interface {
	Fetch(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error
}

type remoteCarFetcher struct {
	httpClient *http.Client
	gatewayURL []string
	rand       *rand.Rand
}

// NewRemoteCarFetcher returns a [CarFetcher] that is backed by one or more gateways
// that support partial [CAR requests], as described in [IPIP-402]. You can optionally
// pass your own [http.Client].
//
// [CAR requests]: https://www.iana.org/assignments/media-types/application/vnd.ipld.car
// [IPIP-402]: https://specs.ipfs.tech/ipips/ipip-0402
func NewRemoteCarFetcher(gatewayURL []string, httpClient *http.Client) (CarFetcher, error) {
	if len(gatewayURL) == 0 {
		return nil, errors.New("missing gateway URLs to which to proxy")
	}

	if httpClient == nil {
		httpClient = newRemoteHTTPClient()
	}

	return &remoteCarFetcher{
		gatewayURL: gatewayURL,
		httpClient: httpClient,
		rand:       rand.New(rand.NewSource(time.Now().Unix())),
	}, nil
}

func (ps *remoteCarFetcher) Fetch(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error {
	url := contentPathToCarUrl(path, params)

	urlStr := fmt.Sprintf("%s%s", ps.getRandomGatewayURL(), url.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return err
	}
	log.Debugw("car fetch", "url", req.URL)
	req.Header.Set("Accept", "application/vnd.ipld.car;order=dfs;dups=y")
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		errData, err := io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("could not read error message: %w", err)
		} else {
			err = fmt.Errorf("%q", string(errData))
		}
		return fmt.Errorf("http error from car gateway: %s: %w", resp.Status, err)
	}

	err = cb(path, resp.Body)
	if err != nil {
		resp.Body.Close()
		return err
	}
	return resp.Body.Close()
}

func (ps *remoteCarFetcher) getRandomGatewayURL() string {
	return ps.gatewayURL[ps.rand.Intn(len(ps.gatewayURL))]
}

// contentPathToCarUrl returns an URL that allows retrieval of specified resource
// from a trustless gateway that implements IPIP-402
func contentPathToCarUrl(path path.ImmutablePath, params CarParams) *url.URL {
	return &url.URL{
		Path:     path.String(),
		RawQuery: carParamsToString(params),
	}
}

// carParamsToString converts CarParams to URL parameters compatible with IPIP-402
func carParamsToString(params CarParams) string {
	paramsBuilder := strings.Builder{}
	paramsBuilder.WriteString("format=car") // always send explicit format in URL, this  makes debugging easier, even when Accept header was set
	if params.Scope != "" {
		paramsBuilder.WriteString("&dag-scope=")
		paramsBuilder.WriteString(string(params.Scope))
	}
	if params.Range != nil {
		paramsBuilder.WriteString("&entity-bytes=")
		paramsBuilder.WriteString(strconv.FormatInt(params.Range.From, 10))
		paramsBuilder.WriteString(":")
		if params.Range.To != nil {
			paramsBuilder.WriteString(strconv.FormatInt(*params.Range.To, 10))
		} else {
			paramsBuilder.WriteString("*")
		}
	}
	return paramsBuilder.String()
}

type retryCarFetcher struct {
	inner   CarFetcher
	retries int
}

// NewRetryCarFetcher returns a [CarFetcher] that retries to fetch up to the given
// [allowedRetries] using the [inner] [CarFetcher]. If the inner fetcher returns
// an [ErrPartialResponse] error, then the number of retries is reset to the initial
// maximum allowed retries.
func NewRetryCarFetcher(inner CarFetcher, allowedRetries int) (CarFetcher, error) {
	if allowedRetries <= 0 {
		return nil, errors.New("number of retries must be a number larger than 0")
	}

	return &retryCarFetcher{
		inner:   inner,
		retries: allowedRetries,
	}, nil
}

func (r *retryCarFetcher) Fetch(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error {
	return r.fetch(ctx, path, params, cb, r.retries)
}

func (r *retryCarFetcher) fetch(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback, retriesLeft int) error {
	err := r.inner.Fetch(ctx, path, params, cb)
	if err == nil {
		return nil
	}

	if retriesLeft > 0 {
		retriesLeft--
	} else {
		return fmt.Errorf("retry fetcher out of retries: %w", err)
	}

	switch t := err.(type) {
	case ErrPartialResponse:
		if len(t.StillNeed) > 1 {
			return errors.New("only a single request at a time is supported")
		}

		// Resets the number of retries for partials, mimicking Caboose logic.
		retriesLeft = r.retries

		return r.fetch(ctx, t.StillNeed[0].Path, t.StillNeed[0].Params, cb, retriesLeft)
	default:
		return r.fetch(ctx, path, params, cb, retriesLeft)
	}
}
