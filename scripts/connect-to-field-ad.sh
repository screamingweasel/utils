################################################################################################
# Connect a field cloud instance to the share AD domain and sync using sssd
################################################################################################
sudo yum -y --enablerepo=extras install epel-release
sudo yum install -y -q curl sssd oddjob-mkhomedir authconfig sssd-krb5 sssd-ad sssd-tools
sudo yum install -y -q adcli
sudo yum install -y -q ntp ntpdate openssl openssh-clients
sudo yum install -y -q openldap-clients
sudo yum install -y -q krb5-workstation
 
# Probably not needed, but make sure it is correct 
sudo tee /etc/krb5.conf > /dev/null <<EOF
[libdefaults]
  renew_lifetime = 7d
  forwardable = true
  default_realm = FIELD.HORTONWORKS.COM
  ticket_lifetime = 24h
  dns_lookup_realm = false
  dns_lookup_kdc = false
  default_ccache_name = /tmp/krb5cc_%{uid}
  #default_tgs_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5
  #default_tkt_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5
 
[domain_realm]
  .field.hortonworks.com = FIELD.HORTONWORKS.COM
  field.hortonworks.com = FIELD.HORTONWORKS.COM
 
[logging]
  default = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
  kdc = FILE:/var/log/krb5kdc.log
 
[realms]
  FIELD.HORTONWORKS.COM = {
    admin_server = ad01.field.hortonworks.com
    admin_server = ad02.field.hortonworks.com
    kdc = ad01.field.hortonworks.com
    kdc = ad02.field.hortonworks.com
  }
EOF
 
# Join the domain
sudo echo "BadPass#1" | sudo kinit registersssd@FIELD.HORTONWORKS.COM
sudo adcli join -v \
  --domain-controller=ad01.field.hortonworks.com \
  --domain-ou="OU=HadoopNodes,DC=field,DC=hortonworks,DC=com" \
  --login-ccache="/tmp/krb5cc_0" \
  --login-user="registersssd@FIELD.HORTONWORKS.COM" \
  -v \
  --show-details
 
sudo tee /etc/sssd/sssd.conf > /dev/null <<EOF
[sssd]
#debug_level = 9
## master & data nodes only require nss. Edge nodes require pam.
services = nss, pam, ssh, autofs, pac
config_file_version = 2
domains = FIELD.HORTONWORKS.COM
override_space = _
 
[domain/FIELD.HORTONWORKS.COM]
#debug_level = 9
id_provider = ad
ad_server = ad01.field.hortonworks.com,ad02.field.hortonworks.com
#ad_server = ad01, ad02, ad03
#ad_backup_server = ad-backup01, 02, 03
auth_provider = ad
chpass_provider = ad
access_provider = ad
enumerate = False
krb5_realm = FIELD.HORTONWORKS.COM
ldap_schema = ad
ldap_id_mapping = True
cache_credentials = True
ldap_access_order = expire
ldap_account_expire_policy = ad
ldap_force_upper_case_realm = true
fallback_homedir = /home/%d/%u
default_shell = /bin/false
ldap_referrals = false
ignore_group_members = true
 
[nss]
debug_level = 9
memcache_timeout = 3600
override_shell = /bin/bash
EOF
 
sudo chmod 0600 /etc/sssd/sssd.conf
sudo service sssd restart
sudo authconfig --enablesssd --enablesssdauth --enablemkhomedir --enablelocauthorize --update
sudo chkconfig oddjobd on
sudo service oddjobd restart
sudo chkconfig sssd on
sudo service sssd restart
sudo kdestroy