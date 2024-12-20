#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

jsons2avro2cbor(){
	cat sample.d/sample.jsonl |
		json2avrows |
		rq \
			--input-avro \
			--output-cbor |
		cat > ./sample.d/input.cbor
}

#jsons2avro2cbor

cat sample.d/input.cbor |
	./cbors2avro |
	rq \
		--input-avro \
		--output-json |
	jaq -c
