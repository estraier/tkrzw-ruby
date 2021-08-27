#! /bin/sh

set -eux

PACKAGE_NAME="tkrzw-ruby-0.1.21"

LANG=C
LC_ALL=C
PATH="$PATH:/usr/local/bin:$HOME/bin:.:.."
export LANG LC_ALL PATH

if [ -f Makefile ]
then
  make distclean
fi

rm -rf casket casket* *~ *.tmp *.tkh *.tkt *.tks *.flat *.log *.gem hoge moge tako ika uni
rm -rf api-doc

cd ..
if [ -d tkrzw-ruby ]
then
  rm -Rf "${PACKAGE_NAME}"
  cp -R tkrzw-ruby "${PACKAGE_NAME}"
  tar --exclude=".*" -cvf - "${PACKAGE_NAME}" | gzip -c > "${PACKAGE_NAME}.tar.gz"
  rm -Rf "${PACKAGE_NAME}"
fi
