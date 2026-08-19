package api

import (
	"encoding/xml"
	"io"
	"net/http"
	"time"
)

const (
	s3Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	// timeFmt is the ISO8601 format S3 uses in XML bodies.
	timeFmt = "2006-01-02T15:04:05.000Z"
)

func xmlTime(t time.Time) string { return t.UTC().Format(timeFmt) }

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header) //nolint:errcheck
	xml.NewEncoder(w).Encode(v)   //nolint:errcheck
	io.WriteString(w, "\n")       //nolint:errcheck
}

type xmlOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type xmlBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   xmlOwner `xml:"Owner"`
	Buckets struct {
		Bucket []xmlBucket `xml:"Bucket"`
	} `xml:"Buckets"`
}

type xmlObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type listBucketResultV2 struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []xmlObject       `xml:"Contents"`
	CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes"`
}

type listBucketResultV1 struct {
	XMLName        xml.Name          `xml:"ListBucketResult"`
	Xmlns          string            `xml:"xmlns,attr"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	EncodingType   string            `xml:"EncodingType,omitempty"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []xmlObject       `xml:"Contents"`
	CommonPrefixes []xmlCommonPrefix `xml:"CommonPrefixes"`
}

type xmlVersion struct {
	Key          string   `xml:"Key"`
	VersionID    string   `xml:"VersionId"`
	IsLatest     bool     `xml:"IsLatest"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
	StorageClass string   `xml:"StorageClass"`
	Owner        xmlOwner `xml:"Owner"`
}

type listVersionsResult struct {
	XMLName             xml.Name          `xml:"ListVersionsResult"`
	Xmlns               string            `xml:"xmlns,attr"`
	Name                string            `xml:"Name"`
	Prefix              string            `xml:"Prefix"`
	KeyMarker           string            `xml:"KeyMarker"`
	VersionIDMarker     string            `xml:"VersionIdMarker"`
	NextKeyMarker       string            `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string            `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int               `xml:"MaxKeys"`
	Delimiter           string            `xml:"Delimiter,omitempty"`
	EncodingType        string            `xml:"EncodingType,omitempty"`
	IsTruncated         bool              `xml:"IsTruncated"`
	Versions            []xmlVersion      `xml:"Version"`
	CommonPrefixes      []xmlCommonPrefix `xml:"CommonPrefixes"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type xmlPart struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type listPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	Initiator            xmlOwner  `xml:"Initiator"`
	Owner                xmlOwner  `xml:"Owner"`
	StorageClass         string    `xml:"StorageClass"`
	Parts                []xmlPart `xml:"Part"`
}

type xmlUpload struct {
	Key          string   `xml:"Key"`
	UploadID     string   `xml:"UploadId"`
	Initiator    xmlOwner `xml:"Initiator"`
	Owner        xmlOwner `xml:"Owner"`
	StorageClass string   `xml:"StorageClass"`
	Initiated    string   `xml:"Initiated"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name    `xml:"ListMultipartUploadsResult"`
	Xmlns              string      `xml:"xmlns,attr"`
	Bucket             string      `xml:"Bucket"`
	KeyMarker          string      `xml:"KeyMarker"`
	UploadIDMarker     string      `xml:"UploadIdMarker"`
	NextKeyMarker      string      `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string      `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads         int         `xml:"MaxUploads"`
	Prefix             string      `xml:"Prefix"`
	IsTruncated        bool        `xml:"IsTruncated"`
	Uploads            []xmlUpload `xml:"Upload"`
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Deleted []deletedEntry `xml:"Deleted"`
	Errors  []deleteError  `xml:"Error"`
}

type deletedEntry struct {
	Key string `xml:"Key"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
