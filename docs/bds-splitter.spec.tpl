%define debug_package %{nil}

Name:       bds-splitter
Version:    GIT_VERSION
Release:    GIT_RELEASE%{?dist}
Summary:    BDS Splitter Application
Group:      Application/Engineering
License:    MIT
URL:        http://bds.jd.com
Source0:    themis.tar.gz
BuildRoot:  %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

%description
BDS API Application.

%prep
%setup -n themis

%build

%install
%{__install} -d %{buildroot}/usr/bin/
%{__install} -d %{buildroot}/etc/bds-splitter/
%{__install} -d %{buildroot}/var/lock/bds-splitter/
%{__install} -d %{buildroot}/var/run/bds-splitter/
%{__install} -d %{buildroot}/var/log/bds-splitter/
%{__install} -d %{buildroot}/usr/share/bds-splitter/
%{__install} -c -m 755 bin/bds-splitter %{buildroot}/usr/bin/bds-splitter
%{__install} -c -m 644 config/splitter_example.conf %{buildroot}/etc/bds-splitter/splitter.conf

%post
if [ $1 -ge 1 ] ; then
    /usr/bin/bds-splitter install
    chkconfig bds-splitter on
fi

%preun
if [ $1 -eq 0 ] ; then
    chkconfig bds-splitter off
    /usr/bin/bds-splitter uninstall
fi

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%attr(0644,nobody,nobody) %config(noreplace) /etc/bds-splitter/splitter.conf
%attr(0755,nobody,nobody) /usr/bin/bds-splitter
%attr(0755,nobody,nobody) /var/lock/bds-splitter/
%attr(0755,nobody,nobody) /var/run/bds-splitter/
%attr(0755,nobody,nobody) /var/log/bds-splitter/

%doc

%changelog
