package datatype

import "net/url"

var AnyURI = baseType{
	GetDerivedDescription: zeroGetDerivedDescription,
	SetValueConstraints:   zeroSetValueConstraints,
	ToGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		// We don't want the rather lax parsing of url.Parse.
		/*
			_, err = url.ParseRequestURI(s)
			if err != nil {
				return nil, err
			}
		*/
		return u, nil
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		u := x.(*url.URL)
		return u.String(), nil
	},
	SqlType: "TEXT",
	ToSql: func(dt *Datatype, x any) (any, error) {
		u := x.(*url.URL)
		return u.String(), nil
	},
}
