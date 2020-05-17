package publisher

type Publisher interface {
	PublishArtistID(ID string) error
	PublishAlbumID(ID string) error
}
