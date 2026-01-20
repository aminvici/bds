CODENAME = themis
PACKAGE  = github/jdcloud-bri/$(CODENAME)
DATE     ?= $(shell date +%Y%m%d)
TIME     ?= $(shell date +%H%M%S)
VERSION  ?= $(shell git describe --tags --always --match=v* | awk -F\- '{print $$1}' | sed -e 's/^v//')
RELEASE  ?= $(shell git describe --tags --always --match=v* | awk -F\- '{print $$2}')
COMMITID ?= $(shell git describe --tags --always --match=v* | awk -F\- '{print $$3}' | sed -e 's/^g//')

GOPATH   = $(CURDIR)/build
BIN      = $(GOPATH)/bin
BASE     = $(GOPATH)/src/$(PACKAGE)
RPMBUILD = $(GOPATH)/rpmbuild
PKGS     = $(or $(PKG),$(shell cd $(BASE) && env GOPATH=$(GOPATH) $(GO) list ./... | grep "^$(PACKAGE)/cmd/"))
TESTPKGS = $(shell env GOPATH=$(GOPATH) $(GO) list -f '{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' $(PKGS))

export GOPATH
export GOBIN = $(BIN)

GO      = go
GODOC   = godoc
GOFMT   = gofmt
TIMEOUT = 15

V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")

# All
.PHONY: all
all: fmt | build

# Build
.PHONY: build
build: $(BASE) ; $(info $(M) building executable ...) @ ## Build program binary
	$Q cd $(BASE) && \
		release=$(RELEASE).$(DATE).$(COMMITID); \
		if [ -z "$(RELEASE)" ] ; then \
			release=0.$(DATE)$(TIME); \
		fi && \
		$(GO) install \
		-tags release \
		-ldflags "-X main.versionNumber=$(VERSION)-$$release -X main.buildTime=$(DATE)-$(TIME)" \
		$(PKGS) ; \
		cp -rdp config/*.conf $(GOPATH)/config 2>/dev/null || true ; \

$(BASE): ; $(info $(M) setting GOPATH ...)
	@mkdir -p $(dir $@)
	@mkdir -p $(GOPATH)/config
	@ln -sf $(CURDIR) $@

# Tools
$(BIN):
	@mkdir -p $@
$(BIN)/%: $(BIN) | $(BASE) ; $(info $(M) building $(REPOSITORY) ...)
	$Q tmp=$$(mktemp -d); \
		(GOPATH=$$tmp go get $(REPOSITORY) && cp $$tmp/bin/* $(BIN)/.) || ret=$$?; \
		rm -rf $$tmp ; exit $$ret

.PHONY: fmt
fmt: ; $(info $(M) running gofmt ...) @ ## Run gofmt on all source files
	@ret=0 && for d in $$($(GO) list -f '{{.Dir}}' ./... | grep -v /vendor/); do \
		$(GOFMT) -l -w $$d/*.go || ret=$$? ; \
	 done ; exit $$ret

# Packages
.PHONY: rpm
rpm: build ; $(info $(M) building rpm ...) @ ## Building rpm packages
	@mkdir -p $(GOPATH)/rpms
	@mkdir -p $(RPMBUILD)/BUILD $(RPMBUILD)/RPMS $(RPMBUILD)/SOURCES $(RPMBUILD)/SPECS $(RPMBUILD)/SRPMS
	@ret=0 && for s in $$(ls spec); do \
		cd $(BASE) ; \
		release=$(RELEASE).$(DATE).$(COMMITID); \
		if [ -z "$(RELEASE)" ] ; then \
			release=0.$(DATE)$(TIME); \
		fi; \
		name=$${s%%.*} ; \
		sed -e "s#GIT_VERSION#$(VERSION)#g" -e "s#GIT_RELEASE#$$release#g" spec/$$s > $(RPMBUILD)/SPECS/$$name.spec ;\
		cd $(GOPATH) ; \
		cp -rdp $(BASE)/config/*.conf config/ ; \
		tar zcfhP $(RPMBUILD)/SOURCES/$(CODENAME).tar.gz $(GOPATH)/bin $(GOPATH)/config --transform s=$(GOPATH)=$(CODENAME)= ; \
		rpmbuild --define '_topdir $(GOPATH)/rpmbuild' -bb $(RPMBUILD)/SPECS/$$name.spec > /dev/null 2>&1 ; \
		echo "$(M) package $$name done ..." ; \
	done && \
	find $(RPMBUILD) -name '*.rpm' | xargs -i cp {} $(GOPATH)/rpms/ && \
	rm -rf $(RPMBUILD) || ret=$$? ; exit $$ret

# Misc
.PHONY: clean
clean: ; $(info $(M) cleaning ...)	@ ## Cleanup everything
	@rm -rf $(GOPATH)
	@rm -rf bin
	@rm -rf test/tests.* test/coverage.*

.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: version
version:
	@ret=0 && \
	release=$(RELEASE).$(DATE).$(COMMITID); \
	if [[ "$(RELEASE)" -eq "" ]] ; then \
		release=0.$(DATE); \
	fi; \
	echo $(VERSION)-$$release
