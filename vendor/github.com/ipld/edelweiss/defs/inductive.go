package defs

type Inductive struct {
	Cases   Cases
	Default DefaultCase
}

type DefaultCase struct {
	GoKeyName   string
	GoValueName string
	Type        Def
}

func (Inductive) Kind() string {
	return "Inductive"
}
