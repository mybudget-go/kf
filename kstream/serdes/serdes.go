package serdes

type Serializer interface {
	Serialize(data interface{}) ([]byte, error)
}

type Deserializer interface {
	Deserialize(data []byte) (interface{}, error)
}

type SerDes interface {
	Serializer
	Deserializer
}
