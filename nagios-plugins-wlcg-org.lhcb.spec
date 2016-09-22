%define site org.lhcb
%define dir %{_libexecdir}/grid-monitoring/probes/%{site}
%define dir2 /usr/lib/ncgx/x_plugins

%define debug_package %{nil}

Summary: WLCG Compliant Probes from %{site}
Name: nagios-plugins-wlcg-org.lhcb
Version: 0.3.7
Release: 1%{?dist}

License: ASL 2.0
Group: Network/Monitoring
Source0: %{name}-%{version}.tgz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
#Requires:
AutoReqProv: no
BuildArch: noarch
BuildRequires: python >= 2.4

%description
IT/SAM nagios probes specific for the LHCb VO

%prep
%setup -q

%build

%install
export DONT_STRIP=1
%{__rm} -rf %{buildroot}
install --directory %{buildroot}%{dir}
install --directory %{buildroot}%{dir2}

%{__cp} -rpf .%dir/wnjob  %{buildroot}%{dir}
%{__cp} -rpf .%dir/SRM-probe  %{buildroot}%{dir}
%{__cp} -rpf .%dir/LFC-probe  %{buildroot}%{dir}
%{__cp} -rpf .%dir/srmvometrics.py  %{buildroot}%{dir}
%{__cp} -rpf .%dir2/lhcb_vofeed.py %{buildroot}%{dir2}
%{__cp} -rpf .%dir2/lhcb_webdav.py %{buildroot}%{dir2}

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{dir}/wnjob
%{dir}/SRM-probe
%{dir}/LFC-probe
%{dir}/srmvometrics.py
%{dir}/srmvometrics.pyc
%{dir}/srmvometrics.pyo
%{dir2}/lhcb_vofeed.py
%{dir2}/lhcb_vofeed.pyc
%{dir2}/lhcb_vofeed.pyo
%{dir2}/lhcb_webdav.py
%{dir2}/lhcb_webdav.pyc
%{dir2}/lhcb_webdav.pyo

%changelog
* Thu Sep 22 2016 Marian Babik <marian.babik@cern.ch> - 0.3.7-1
- Added support for HTCondor-CE
- Added support for hostgroups/notifications

* Tue May 9 2016 Marian Babik <marian.babik@cern.ch> - 0.3.6-3
- New MJF probe
- Added ETF configuration

* Mon Jul 6 2009 C. Triantafyllidis <ctria@grid.auth.gr> - 0.1.0-1
- Initial build

