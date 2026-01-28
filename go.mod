module github.com/aminvici/bds

go 1.25.3

replace github.com/Shopify/sarama => github.com/IBM/sarama v1.46.1

replace github.com/greenplum-db/pq => github.com/lib/pq v1.10.4

replace github.com/kataras/iris => gopkg.in/kataras/iris.v11 v11.1.1

replace github.com/go-ini/ini => gopkg.in/ini.v1 v1.42.0

require (
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/go-ini/ini v0.0.0-00010101000000-000000000000
	github.com/go-sql-driver/mysql v1.9.3
	github.com/go-xorm/xorm v0.7.9
	github.com/greenplum-db/pq v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.18.0
	github.com/twmb/franz-go/pkg/kadm v1.14.0
	github.com/xeipuuv/gojsonschema v1.2.0
	golang.org/x/sys v0.40.0
	xorm.io/core v0.7.3
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.17 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/smartystreets/goconvey v1.8.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	gopkg.in/ini.v1 v1.67.1 // indirect
	xorm.io/builder v0.3.11-0.20220531020008-1bd24a7dc978 // indirect
)
