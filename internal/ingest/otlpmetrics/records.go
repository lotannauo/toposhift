package otlpmetrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The attribute keys the writers read.
const (
	KeyServiceName       = string(catalog.ServiceName)
	KeyServiceNamespace  = string(catalog.ServiceNamespace)
	KeyServiceInstanceID = string(catalog.ServiceInstanceID)
	KeyPodUID            = string(catalog.K8sPodUID)
)

// UsableID says whether a string can be an identity value as received: it is not
// blank, it is valid UTF-8 and it is short enough for the identity encoding. Nothing
// is trimmed.
func UsableID(s string) bool {
	return strings.TrimSpace(s) != "" && utf8.ValidString(s) && len(s) <= identity.MaxValueLen
}

// UsableNamespace says whether a service namespace can be used. An empty one is
// absent, which is usable; a blank one is not.
func UsableNamespace(s string) bool { return s == "" || UsableID(s) }

// Fingerprints makes the fingerprints of the entities the two writers assert, so
// that a service has the same identity and, with [ServiceDescription], the same
// description whichever of them writes it.
type Fingerprints struct {
	r *identity.Resolver
}

// NewFingerprints returns the fingerprints of a resolver, which must not be nil.
func NewFingerprints(r *identity.Resolver) Fingerprints { return Fingerprints{r: r} }

// Service is the fingerprint of a service: its namespace, which is empty if there
// is none, and its name.
func (f Fingerprints) Service(namespace, name string) (identity.Fingerprint, error) {
	return f.resolve(catalog.Service, namespaceAttrs(namespace, name))
}

// Instance is the fingerprint of a service instance.
func (f Fingerprints) Instance(namespace, name, id string) (identity.Fingerprint, error) {
	return f.resolve(catalog.ServiceInstance, append(namespaceAttrs(namespace, name),
		identity.Attr{Key: catalog.ServiceInstanceID, Value: id}))
}

// Pod is the fingerprint of the pod with a uid.
func (f Fingerprints) Pod(uid string) (identity.Fingerprint, error) {
	return f.resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: uid}})
}

// serviceDescription is the payload of a service.
type serviceDescription struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// ServiceDescription is the payload of a service: its name and, when it has one, its
// namespace, the values its identity is made of, as canonical JSON: the keys in that
// order, no HTML escaping and no trailing newline. Both writers build a service's
// payload here, so one service has the same bytes from either. The values must be the
// ones [Fingerprints.Service] accepted.
func ServiceDescription(namespace, name string) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(serviceDescription{Name: name, Namespace: namespace}); err != nil {
		// Only strings are encoded.
		panic("otlpmetrics: encoding a service description: " + err.Error())
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}

func namespaceAttrs(namespace, name string) []identity.Attr {
	attrs := make([]identity.Attr, 0, 3)
	if namespace != "" {
		attrs = append(attrs, identity.Attr{Key: catalog.ServiceNamespace, Value: namespace})
	}
	return append(attrs, identity.Attr{Key: catalog.ServiceName, Value: name})
}

func (f Fingerprints) resolve(t catalog.EntityType, attrs []identity.Attr) (identity.Fingerprint, error) {
	id, err := f.r.Resolve(t, attrs)
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("otlpmetrics: %s: %w", t, err)
	}
	return id.Fingerprint(), nil
}

// Layer is the layer a record about the subject is stored in: an entity takes its
// type's layer, and an edge takes the layer its relation is stored in, which the
// catalog records. A writer takes the layer from here and never names one.
func Layer(subj store.Subject) catalog.Layer {
	if subj.Kind != store.SubjectEdge {
		e, ok := catalog.Default().Entity(subj.A.Type())
		if !ok {
			// The resolver and the default catalog list the same types.
			panic("otlpmetrics: entity type " + string(subj.A.Type()) + " is not in the default catalog")
		}
		return e.Layer()
	}
	r, ok := catalog.Default().Relation(subj.Relation)
	if !ok || r.Derived() {
		panic("otlpmetrics: relation " + string(subj.Relation) + " is not a stored relation of the default catalog")
	}
	return r.Layer()
}

// Observe is an Observe of a subject at an instant, with a TTL, in the subject's
// layer. It is the only record a metric writer makes: there is no Delete and no
// Through. The payload is copied.
func Observe(producer lifecycle.Producer, subj store.Subject, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte) store.Record {
	var p []byte
	if len(payload) > 0 {
		p = append(p, payload...)
	}
	return store.Record{
		Layer:          Layer(subj),
		Subject:        subj,
		Producer:       producer,
		EventTime:      at,
		Kind:           lifecycle.Observe,
		TTL:            ttl,
		Payload:        p,
		EventTimeBasis: basis,
	}
}
