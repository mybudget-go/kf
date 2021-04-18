package serdes

type StringSerializer struct{}

func (b StringSerializer) Serialize(data interface{}) ([]byte, error) {
	return []byte(data.(string)), nil
}

func (b StringSerializer) Deserialize(data []byte) (interface{}, error) {
	return string(data), nil
}
