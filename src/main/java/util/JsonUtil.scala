package util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

object JsonUtil {

	def format(o: Object): String = {
		new ObjectMapper().writeValueAsString(o)
	}
	def parse(json: String): JsonNode = {
		new ObjectMapper().readTree(json)
	}

}
