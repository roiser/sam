%define site org.lhcb
%define dir %{_libexecdir}/grid-monitoring/probes/%{site}
%define dir2 /etc

%define debug_package %{nil}

Summary: WLCG Compliant Probes from %{site}
Name: nagios-plugins-wlcg-org.lhcb
Version: 0.3.4
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
%{__cp} -rpf .%dir2/ncg-metric-config.d %{buildroot}%{dir2}

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
%{dir2}/ncg-metric-config.d

%changelog
* Mon Jul 6 2009 C. Triantafyllidis <ctria@grid.auth.gr> - 0.1.0-1
- Initial build

