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
	"github.com/gophercloud/gophercloud/pagination"

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
	openstackTenantName = "privateOpenstackTenantName"
	openstackUsername   = "privateOpenstackUsername"
	openstackPassword   = "privateOpenstackPassword"
)

var requiredOpts = []string{
	openstackRegion,
	openstackContainer,
	openstackAuthURL,
	openstackTenantName,
	openstackUsername,
	openstackPassword,
}

// See https://docs.openstack.org/swift/latest/overview_acl.html
const containerPublicACL = ".r:*"

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

var (
	_ storage.Storage = (*openstackStorage)(nil)
	_ storage.Lister  = (*openstackStorage)(nil)
)

// LinkBase will return the URL if the container has read access for everybody
// and an unsupported error in case it does not. Still, it might return an
// error because it can't get the necessary metadata.
func (s *openstackStorage) LinkBase() (string, error) {
	const op = "cloud/storage/openstack.LinkBase"

	r := containers.Get(s.client, s.container)
	h, err := r.Extract()
	if err != nil {
		return "", errors.E(op, errors.Internal, errors.Errorf(
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

func (s *openstackStorage) pager(url string, perPage int) pagination.Pager {
	// First page, can use objects.List().
	if url == "" {
		return objects.List(s.client, s.container, objects.ListOpts{
			Full:  true,
			Limit: perPage,
		})
	}
	// Continuation page, need custom pager.
	return pagination.NewPager(s.client, url, func(r pagination.PageResult) pagination.Page {
		p := objects.ObjectPage{
			MarkerPageBase: pagination.MarkerPageBase{PageResult: r},
		}
		p.MarkerPageBase.Owner = p
		return p
	})
}

func (s *openstackStorage) list(url string, perPage int) (refs []upspin.ListRefsItem, nextToken string, err error) {
	const op = "cloud/storage/openstack.List"

	pager := s.pager(url, perPage)

	err = pager.EachPage(func(page pagination.Page) (bool, error) {
		objs, err := objects.ExtractInfo(page)
		if err != nil {
			return false, err
		}
		for _, o := range objs {
			refs = append(refs, upspin.ListRefsItem{
				Ref:  upspin.Reference(o.Name),
				Size: o.Bytes,
			})
		}
		token, err := page.NextPageURL()
		if err != nil {
			return false, err
		}
		nextToken = token

		// Stop pagination after the first page. Let the Upspin client
		// do the pagination.  If we called pager.AllPages() we'd have
		// to wait until all pagination is done and the client would
		// probably give up waiting for a response.
		return false, nil
	})

	if err != nil {
		err = errors.E(op, errors.IO, errors.Errorf("%q: %v", s.container, err))
	}

	return
}

// List implements storage.Lister. In this implementation, the token is in fact
// the URL for the next page.
func (s *openstackStorage) List(token string) ([]upspin.ListRefsItem, string, error) {
	return s.list(token, 0)
}
