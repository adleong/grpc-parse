package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
)

const (
	Varint      = 0
	B64         = 1
	LengthDelim = 2
	B32         = 5
)

type (
	Field struct {
		numeric *uint64
		string  *string
		message *Message
		bytes   *[]byte
	}

	Message = map[uint64][]Field

	Tag struct {
		fieldID uint64
		typ     uint64
	}
)

func addField(m Message, id uint64, field Field) {
	fields, ok := m[id]
	if ok {
		m[id] = append(fields, field)
	} else {
		m[id] = []Field{field}
	}
}

func ParseGrpc(data []byte) (*Message, int, error) {
	if len(data) < 5 {
		return nil, 0, fmt.Errorf("Missing gRPC frame size, only %d bytes available", len(data))
	}
	size := 0
	for i := 0; i < 5; i++ {
		size = size << 8
		size += int(data[i])
	}
	data = data[5:]
	if len(data) < int(size) {
		return nil, 0, fmt.Errorf("Incomplete gRPC frame, wanted %d bytes but only found %d", size, len(data))
	}
	msg, n, err := ParseProto(data[:size])
	return msg, n + 5, err
}

func ParseProto(data []byte) (*Message, int, error) {
	pos := 0
	msg := make(Message)
	for pos < len(data) {
		tag, n, err := ParseTag(data[pos:])
		if err != nil {
			return nil, 0, err
		}
		pos += n
		switch tag.typ {
		case Varint:
			x, n := proto.DecodeVarint(data[pos:])
			addField(msg, tag.fieldID, Field{
				numeric: &x,
			})
			pos += n
		case B32:
			buf := proto.NewBuffer(data[pos:])
			x, err := buf.DecodeFixed32()
			if err != nil {
				return nil, 0, err
			}
			addField(msg, tag.fieldID, Field{
				numeric: &x,
			})
			pos += 4
		case B64:
			buf := proto.NewBuffer(data[pos:])
			x, err := buf.DecodeFixed64()
			if err != nil {
				return nil, 0, err
			}
			addField(msg, tag.fieldID, Field{
				numeric: &x,
			})
			pos += 8
		case LengthDelim:
			x, n := proto.DecodeVarint(data[pos:])
			pos += n
			if pos+int(x) > len(data) {
				return nil, 0, fmt.Errorf("Not enough bytes for length delimited field, wanted %d but only found %d", x, len(data)-pos)
			}
			content := data[pos : pos+int(x)]
			pos += int(x)
			if subMsg, _, err := ParseProto(content); err == nil {
				addField(msg, tag.fieldID, Field{
					message: subMsg,
				})
			} else if utf8.Valid(content) {
				str := string(content)
				addField(msg, tag.fieldID, Field{
					string: &str,
				})
			} else {
				addField(msg, tag.fieldID, Field{
					bytes: &content,
				})
			}
		}
	}
	return &msg, pos, nil
}

func ParseTag(data []byte) (*Tag, int, error) {
	x, n := proto.DecodeVarint(data)
	typ := x & 7
	id := x >> 3
	switch typ {
	case Varint:
		fallthrough
	case B64:
		fallthrough
	case LengthDelim:
		fallthrough
	case B32:
		return &Tag{
			fieldID: id,
			typ:     typ,
		}, n, nil
	default:
		return nil, 0, fmt.Errorf("Invalid field type: %d", typ)
	}
}

func Render(m *Message) string {
	out := []string{}
	for id, fields := range *m {
		if len(fields) == 1 {
			out = append(out, fmt.Sprintf("\"%d\":%s", id, RenderField(fields[0])))
		} else {
			repeated := []string{}
			for _, f := range fields {
				repeated = append(repeated, RenderField(f))
			}
			out = append(out, fmt.Sprintf("\"%d\":[%s]", id, strings.Join(repeated, ",")))
		}
	}
	return fmt.Sprintf("{%s}", strings.Join(out, ","))
}

func RenderField(f Field) string {
	if f.numeric != nil {
		return fmt.Sprintf("%d", *f.numeric)
	}
	if f.string != nil {
		return fmt.Sprintf("\"%s\"", *f.string)
	}
	if f.message != nil {
		return Render(f.message)
	}
	if f.bytes != nil {
		return fmt.Sprintf("%x", f.bytes)
	}
	return ""
}

func main() {

	data, _ := ioutil.ReadAll(os.Stdin)

	//data := []byte{0x00, 0x00, 0x00, 0x00, 0x1f, 0x0a, 0x0a, 0x0a, 0x08, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x12, 0x0f, 0x32, 0x0d, 0x0a, 0x0b, 0x0a, 0x09, 0x73, 0x65, 0x72, 0x69, 0x65, 0x73, 0x5f, 0x76, 0x31, 0x1a, 0x00}

	msg, _, _ := ParseGrpc(data)
	fmt.Println(Render(msg))
}
