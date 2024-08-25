# Required Assets for the Gateway

> HTTP Gateway Templates.

## Updating

To update the templates, make changes to `.html` and `.css` files in the current directory.

## Testing

1. Make sure you have [Go](https://golang.org/dl/) installed.
2. From the `assets/` directory, start the test server: `go run test/main.go`.

This will listen on [`localhost:3000`](http://localhost:3000/) and reload the template every time you refresh the page. Here you have three pages:

- [`localhost:3000/dag`](http://localhost:3000/dag) for the DAG template preview; and
- [`localhost:3000/directory`](http://localhost:3000/directory) for the Directory template preview; and
- [`localhost:3000/error?code=500`](http://localhost:3000/error?status=500) for the Error template preview, you can replace `500` by a different status code.

Every time you refresh, the template will be reloaded.
