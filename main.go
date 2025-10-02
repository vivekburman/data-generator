package main

import (
	generator "datagenerator/generator/elastic"
)

func main() {
	// generator.MariaDB()
	// generator.MySQL()
	// generator.Postgres()
	// generator.MSSQL()
	// generator.Oracle()
	// generator.Redis()
	// generator.MongoDB()
	// generator.MySQLRelational()
	// generator.PostgresRelational()
	// generator.OracleRelational()
	// generator.MSQLRelational()
	// generator.MSSQLECommerceOrderBroker()
	// generator.Elasticsearch()
	// for range 10 {
	// 	generator.PerformSeed()
	// }
	generator.CreateElasticsearchSchema()
}
