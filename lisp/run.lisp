#!/usr/bin/env -S sbcl --dynamic-space-size 16GiB --script
(load (sb-ext:posix-getenv "ASDF"))
(push (truename ".") asdf:*central-registry*)
(asdf:load-system 'gnip)
(gnip::run)
