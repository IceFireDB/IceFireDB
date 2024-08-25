# IPFS `_redirects` File Parser

This is a parser for the IPFS Web Gateway's `_redirects` file format.

## Specification

Follow specification work at https://github.com/ipfs/specs/pull/290

## Format
Currently only supports `from`, `to` and `status`.

```
from to [status]
```

## Example

```sh
# Implicit 301 redirects
/home              /
/blog/my-post.php  /blog/my-post
/news              /blog
/google            https://www.google.com

# Redirect with a 301
/home         /              301

# Redirect with a 302
/my-redirect  /              302

# Redirect with wildcard (splat placeholder)
/splat/* /redirected-splat/:splat 301

# Redirect with multiple named placeholder
/posts/:year/:month/:day/:title  /articles/:year/:month/:day/:title  301

# Show a custom 404 for everything under this path
/ecommerce/*  /store-closed.html  404

# Single page app rewrite (SPA, PWA)
/*    /index.html   200
```

## Notes for contributors

- `make all` builds and runs tests
- `FUZZTIME=1m make fuzz` runs fuzzing for specified amount of time

---

## Credit
This project was forked from [tj/go-redirects](https://github.com/tj/go-redirects).  Thank you TJ for the initial work. 🙏
