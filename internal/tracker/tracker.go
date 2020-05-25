package tracker

type Tracker interface {
	UpdateArtist(artistID string) error
	UpdateAlbum(albumID string) error
	ArtistIsStale(artistID string) bool
	AlbumIsStale(albumID string) bool
}
