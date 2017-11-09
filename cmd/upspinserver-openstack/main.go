// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command upspinserver-openstack is a combined DirServer and
// StoreServer for use on stand-alone machines. It provides the
// production implementations of the dir and store servers
// (dir/server and store/server) with support for storage in
// OpenStack e.g. OVH Object Storage.
package main // import "openstack.upspin.io/cmd/upspinserver-openstack"

import (
	_ "openstack.upspin.io/cloud/storage/openstack"

	"upspin.io/cloud/https"
	"upspin.io/serverutil/upspinserver"
)

func main() {
	ready := upspinserver.Main()
	https.ListenAndServe(ready, https.OptionsFromFlags())
}
