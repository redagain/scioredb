package scioredb

import (
	"io"
	"testing"
)

func TestGenericSeekerSeek(t *testing.T) {
	type fields struct {
		Off  int64
		Size int64
	}
	type args struct {
		offset int64
		whence int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantPos int64
		wantErr bool
	}{
		{
			name: "InvalidWhence",
			args: args{
				whence: 3,
			},
			wantErr: true,
			wantPos: 0,
		},
		{
			name: "NegativePosition",
			args: args{
				whence: io.SeekStart,
				offset: -10,
			},
			wantPos: -10,
			wantErr: true,
		},
		{
			name: "Start",
			fields: fields{
				Off:  0,
				Size: 10,
			},
			args: args{
				offset: 0,
				whence: io.SeekStart,
			},
			wantPos: 0,
		},
		{
			name: "Current",
			fields: fields{
				Off:  0,
				Size: 10,
			},
			args: args{
				offset: 1,
				whence: io.SeekCurrent,
			},
			wantPos: 1,
		},
		{
			name: "End",
			fields: fields{
				Off:  0,
				Size: 10,
			},
			args: args{
				offset: 0,
				whence: io.SeekEnd,
			},
			wantPos: 10,
		},
		{
			name: "NegativeOffset",
			fields: fields{
				Size: 10,
			},
			args: args{
				offset: -1,
				whence: io.SeekEnd,
			},
			wantPos: 9,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := genericSeeker{
				Off:  tt.fields.Off,
				Size: tt.fields.Size,
			}
			gotPos, err := s.Seek(tt.args.offset, tt.args.whence)
			if (err != nil) != tt.wantErr {
				t.Errorf("Seek() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotPos != tt.wantPos {
				t.Errorf("Seek() gotPos = %v, want %v", gotPos, tt.wantPos)
			}
		})
	}
}
