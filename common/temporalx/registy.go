package temporalx

import "fmt"

type Registry struct {
	byName map[string]Definition
}

func NewRegistry(defs ...Definition) (*Registry, error) {
	r := &Registry{
		byName: make(map[string]Definition, len(defs)),
	}

	for _, def := range defs {
		if def.Name() == "" {
			return nil, fmt.Errorf("workflow name is empty")
		}
		if def.TaskQueue() == "" {
			return nil, fmt.Errorf("workflow %s: empty task queue", def.Name())
		}
		if _, exists := r.byName[def.Name()]; exists {
			return nil, fmt.Errorf("duplicate workflow name: %s", def.Name())
		}
		r.byName[def.Name()] = def
	}

	return r, nil
}

func MustRegistry(defs ...Definition) *Registry {
	r, err := NewRegistry(defs...)
	if err != nil {
		panic(err)
	}
	return r
}

func (r *Registry) Resolve(name string) (Definition, bool) {
	def, ok := r.byName[name]
	return def, ok
}
