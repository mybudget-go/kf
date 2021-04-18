package serdes

type ByteArraySerializer struct{}

func (b ByteArraySerializer) Serialize(data interface{}) ([]byte, error) {
	return data.([]byte), nil
}

func (b ByteArraySerializer) Deserialize(data []byte) (interface{}, error) {
	return data, nil
}
