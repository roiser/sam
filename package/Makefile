PKGNAME=grid-monitoring-probes-org.lhcb-0.3.3
TARFILE=$(PKGNAME).tgz

all:
	tar cvzf $(TARFILE) --exclude-vcs --transform 's,^,$(PKGNAME)/,' *

clean:
	rm $(TARFILE)
