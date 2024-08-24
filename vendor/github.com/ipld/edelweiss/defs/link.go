package defs

type Link struct {
	To Def
}

func (Link) Kind() string {
	return "Link"
}
