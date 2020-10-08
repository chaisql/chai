package expr

import "testing"

func Test_like(t *testing.T) {
	type args struct {
		text    string
		pattern string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{"Empty", args{"", ""}, true, false},
		{"Empty false", args{"abc", ""}, false, false},

		{"Any", args{"abc", "%"}, true, false},
		{"Any false", args{"", "%"}, true, false},

		{"Any one and more", args{"abc", "_"}, true, false},
		{"Any one and more false", args{"", "_"}, false, false},

		{"Exact", args{"abc", "abc"}, true, false},
		{"Exact false", args{"abc", "def"}, false, false},

		{"Prefix", args{"abcdef", "abc%"}, true, false},
		{"Prefix false", args{"abcdef", "def%"}, false, false},

		{"Suffix", args{"defabc", "%abc"}, true, false},
		{"Suffix false", args{"defabc", "%def"}, false, false},

		{"Contains", args{"defabcdef", "%abc%"}, true, false},
		{"Contains false", args{"abcd", "%def%"}, false, false},

		{"Regexp", args{"AdBeeeC", "A%B%C"}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := like(tt.args.text, tt.args.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("like() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("like() got = %v, want %v", got, tt.want)
			}
		})
	}
}
