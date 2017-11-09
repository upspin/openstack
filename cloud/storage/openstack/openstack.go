// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package openstack implements a storage backend that saves
// data to an OpenStack container, e.g., OVH Object Storage.
package openstack // import "openstack.upspin.io/cloud/storage/openstack"

import (
	"bytes"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"

	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"
)

const storageName = "OpenStack"

// OpenStack specific option names.
const (
	openstackRegion     = "openstackRegion"
	openstackContainer  = "openstackContainer"
	openstackAuthURL    = "openstackAuthURL"
	openstackTenantName = "openstackTenantName"
	openstackUsername   = "openstackUsername"
	openstackPassword   = "openstackPassword"
)

var (
	requiredOpts = []string{
		openstackRegion,
		openstackContainer,
		openstackAuthURL,
		openstackTenantName,
		openstackUsername,
		openstackPassword,
	}
)

const (
	// See https://docs.openstack.org/swift/latest/overview_acl.html
	containerPublicACL = ".r:*"
)

type openstackStorage struct {
	client    *gophercloud.ServiceClient
	container string
}

// New creates a new instance of the OpenStack implementation of
// storage.Storage.
func New(opts *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/openstack.New"

	for _, opt := range requiredOpts {
		if _, ok := opts.Opts[opt]; !ok {
			return nil, errors.E(op, errors.Invalid, errors.Errorf(
				"%q option is required", opt))
		}
	}

	authOpts := gophercloud.AuthOptions{
		IdentityEndpoint: opts.Opts[openstackAuthURL],
		Username:         opts.Opts[openstackUsername],
		Password:         opts.Opts[openstackPassword],
		TenantName:       opts.Opts[openstackTenantName],
	}

	// When the token expires the services returns 401 and we need to be
	// able to authenticate again.
	authOpts.AllowReauth = true

	provider, err := openstack.AuthenticatedClient(authOpts)
	if err != nil {
		return nil, errors.E(op, errors.Permission, errors.Errorf(
			"Could not authenticate: %s", err))
	}

	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region: opts.Opts[openstackRegion],
	})
	if err != nil {
		// The error kind is "Invalid" because AFAICS this can only
		// happen for unknown region
		return nil, errors.E(op, errors.Invalid, errors.Errorf(
			"Could not create object storage client: %s", err))
	}

	return &openstackStorage{
		client:    client,
		container: opts.Opts[openstackContainer],
	}, nil
}

func init() {
	err := storage.Register(storageName, New)
	if err != nil {
		// If more modules are registering under the same storage name,
		// an application should not start.
		panic(err)
	}
}

var _ storage.Storage = (*openstackStorage)(nil)

// LinkBase will return the URL if the container has read access for everybody
// and an unsupported error in case it does not. Still, it might return an
// error because it can't get the necessary metadata.
func (s *openstackStorage) LinkBase() (string, error) {
	const op = "cloud/storage/openstack.LinkBase"

	r := containers.Get(s.client, s.container)
	h, err := r.Extract()
	if err != nil {
		return "", errors.E(op, errors.IO, errors.Errorf(
			"Unable to extract header: %s", err))
	}
	for _, acl := range h.Read {
		if acl == containerPublicACL {
			return s.client.ServiceURL(s.container) + "/", nil
		}
	}
	return "", upspin.ErrNotSupported
}

func (s *openstackStorage) Download(ref string) ([]byte, error) {
	const op = "cloud/storage/openstack.Download"

	r := objects.Download(s.client, s.container, ref, nil)
	contents, err := r.ExtractContent()
	if err != nil {
		if _, ok := err.(gophercloud.ErrDefault404); ok {
			return nil, errors.E(op, errors.NotExist, err)
		}
		return nil, errors.E(op, errors.IO, errors.Errorf(
			"Unable to download ref %q from container %q: %s", ref, s.container, err))
	}
	return contents, nil
}

func (s *openstackStorage) Put(ref string, contents []byte) error {
	const op = "cloud/storage/openstack.Put"

	opts := objects.CreateOpts{Content: bytes.NewReader(contents)}
	err := objects.Create(s.client, s.container, ref, opts).Err
	if err != nil {
		return errors.E(op, errors.IO, errors.Errorf(
			"Unable to upload ref %q to container %q: %s", ref, s.container, err))
	}
	return nil
}

func (s *openstackStorage) Delete(ref string) error {
	const op = "cloud/storage/openstack.Delete"

	err := objects.Delete(s.client, s.container, ref, nil).Err
	if err != nil {
		return errors.E(op, errors.IO, errors.Errorf(
			"Unable to delete ref %q from container %q: %s", ref, s.container, err))
	}
	return nil
}
