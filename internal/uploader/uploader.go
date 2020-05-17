package uploader

import (
	"github.com/zmb3/spotify"
)

type Uploader interface {
	UploadArtist(artist *spotify.FullArtist) error
	UploadAlbum(album *spotify.FullAlbum) error
}
