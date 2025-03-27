package scioredb

import "testing"

func TestBlockID(t *testing.T) {
	type args struct {
		fileSize  int64
		blockSize int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "0",
			args: args{
				fileSize:  0,
				blockSize: 512,
			},
			want: 0,
		},
		{
			name: "1",
			args: args{
				fileSize:  512,
				blockSize: 512,
			},
			want: 1,
		},
		{
			name: "2",
			args: args{
				fileSize:  1024,
				blockSize: 512,
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BlockID(tt.args.fileSize, tt.args.blockSize); got != tt.want {
				t.Errorf("BlockID() = %v, want %v", got, tt.want)
			}
		})
	}
}
