package cacheaside

type Coder interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
	Name() string
}
