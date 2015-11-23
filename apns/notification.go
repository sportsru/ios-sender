package apns

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

const (
// PayloadMaxSize еhe maximum size allowed for a notification payload was 256 bytes.
// Any notifications larger than this limit are refused by Apple.
//
//  "In iOS 8 and later, the maximum size allowed for a notification payload is 2 kilobytes"
// https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/Chapters/ApplePushService.html
	PayloadMaxSize = 2048
)

// Notify stores payload and limit
type Notify struct {
	Message      Notification
	PayloadLimit int
}

// Notification contents APNS message fields
type Notification struct {
	// Alert message to display to the user.
	Alert string

	// AlertLocKey the localization key for an alert message. `Alert` will be ignored if this is set.
	AlertLocKey string

	// AlertLocArgs strings that replace the format specifiers `%@` and `%n$@` in a localized alert.
	AlertLocArgs []string

	// ActionLocKey the localization key for the right button's title. If the value is `null`,
	// the alert will have a single "OK" button that dismisses the alert when tapped.
	// If not set, a "Close" and "View" button will be displayed.
	ActionLocKey string

	// Badge the number to display as the badge of the application icon. To remove the
	// badge, set the value to `0`. If not set, the badge is not changed.
	Badge interface{}

	// Sound the name of a sound file in the application bundle. If the sound file doesn't
	// exist or the value is set to "default", the default alert sound will be played.
	Sound string

	// LaunchImage the name of an image file in the application bundle. If not set, iOS will use
	// the previous snapshot, the image identified by the `UILaunchImageFile` key in
	// the application's `Info.plist` file, or the `Default.png`.
	LaunchImage string

	// Expiry a Unix timestamp identifying when the notification is no longer valid and
	// can be discarded by the Apple servers if not yet delivered.
	Expiry int64

	// Custom values your app can use to set context for the user interface.
	// You should not include customer information or any sensitive data.
	Custom
}

// Custom contents APNS message Idextra fields
type Custom map[string]interface{}

// NewNotify creates and returns a new notification with all child maps
// and structures pre-initialized.
func NewNotify() *Notify {
	// Pre-make any required maps or other structures.
	return &Notify{
		Message: Notification{
			Custom: Custom{},
		},
		PayloadLimit: PayloadMaxSize,
	}
}

// SetExpiry accepts a Unix timestamp that identifies when the notification
// is no longer valid and can be discarded by the Apple servers if not yet delivered.
func (n *Notify) SetExpiry(expiry int64) {
	n.Message.Expiry = expiry
}

// SetExpiryTime accepts a `time.Time` that identifies when the notification
// is no longer valid and can be discarded by the Apple servers if not yet delivered.
func (n *Notify) SetExpiryTime(t time.Time) {
	n.Message.Expiry = t.Unix()
}

// SetExpiryDuration accepts a `time.Duration` that identifies when the notification
// is no longer valid and can be discarded by the Apple servers if not yet delivered.
// The Duration given will be added to the result of `time.Now()`.
func (n *Notify) SetExpiryDuration(d time.Duration) {
	t := time.Now().Add(d)
	n.Message.Expiry = t.Unix()
}

// toPayload converts a Notification into a map capable of being marshaled into JSON.
// TODO: Maybe this should be renamed? It actually converts to a map.
func (n *Notify) toPayload() (*map[string]interface{}, error) {
	// I don't like going from Struct to Map to JSON, but this is the best solution
	// I can come up with right now to continue keeping the API simple and elegant.
	payload := make(map[string]interface{})
	aps := make(map[string]interface{})

	// FIXME: move to message method
	// aps["alert"] = genAlert(m)
	// There's 3 cases in which we might need to use the alert dictionary format.
	// 1) A localized action key is set (ActionLocKey).
	// 2) A localized alert key is set (AlertLocKey).
	// 3) A custom launch image is set (LaunchImage).
	m := n.Message
	if m.ActionLocKey != "" || m.AlertLocKey != "" || m.LaunchImage != "" {
		alert := make(map[string]interface{})

		// Don't send a body if there is a localized alert key set.
		// TODO: Log a warning about the value of `Alert` being ignored.
		if m.Alert != "" && m.AlertLocKey == "" {
			alert["body"] = m.Alert
		}

		if m.ActionLocKey != "" {
			alert["action-loc-key"] = m.ActionLocKey
		}

		if m.LaunchImage != "" {
			alert["launch-image"] = m.LaunchImage
		}

		if m.AlertLocKey != "" {
			alert["loc-key"] = m.AlertLocKey

			// This check is nested because you can send an alert key without
			// sending any arguments, but not the otherway around.
			if len(m.AlertLocArgs) > 0 {
				alert["loc-args"] = m.AlertLocArgs
			}
		}

		aps["alert"] = &alert
	} else if m.Alert != "" {
		aps["alert"] = m.Alert
	}

	// We use an `interface{}` for `Badge` because the `int` type is always initalized to 0.
	// That means we wouldn't be able to tell if someone had explicitly set `Badge` to 0
	// or if they had not set it at all. This switch checks let's us make sure it was
	// set explicitly, and to an integer, before storing it in the payload.
	switch m.Badge.(type) {
	case nil:
		// If we don't check for the nil case (no badge set), then default will catch it.
		break
	case int:
		aps["badge"] = m.Badge
	default:
		// TODO: Need to check and see if the badge count can be a string, too.
		err := fmt.Errorf("The badge count should be of type `int`, but we found a `%T` instead.", m.Badge)
		return nil, err
	}

	if m.Sound != "" {
		aps["sound"] = m.Sound
	}

	// All standard dictionaries need to be wrapped in the "aps" namespace.
	payload["aps"] = &aps

	// Output all the custom dictionaries.
	for key, value := range m.Custom {
		payload[key] = value
	}

	return &payload, nil
}

// ToJSON generates compact JSON from a notification payload.
func (n *Notify) ToJSON() ([]byte, error) {
	payload, err := n.toPayload()
	if err != nil {
		return nil, err
	}

	return json.Marshal(payload)
}

// ToString generates indented, human readable JSON from a notification payload.
func (n *Notify) ToString() (string, error) {
	payload, err := n.toPayload()
	if err != nil {
		return "", err
	}

	bytes, err := json.MarshalIndent(payload, "", "  ")
	return string(bytes), err
}

// ToBinary converts a JSON payload into a binary format for transmitting to Apple's
// servers over a socket connection.
func (n *Notify) ToBinary(id uint32, token []byte) ([]byte, error) {
	payload, err := n.ToJSON()
	if err != nil {
		return nil, err
	}

	// If the payload is larger than the maximum size allowed by Apple, fail with an error.
	// XXX: Probably we should truncate the "Alert" key instead of completely bailing out. (make it optional?)
	if len(payload) > n.PayloadLimit {
		err := fmt.Errorf("Payload is larger than the %v byte limit: %s", n.PayloadLimit, string(payload))
		return nil, err
	}

	// Create a binary message using the new enhanced format.
	buffer := new(bytes.Buffer)
	data := []interface{}{
		uint8(1),
		id,
		uint32(n.Message.Expiry),
		uint16(len(token)),
		token,
		uint16(len(payload)),
		payload,
	}
	for idx, chunk := range data {
		err = binary.Write(buffer, binary.BigEndian, chunk)
		if err != nil {
			log.Println("binary.Write failed on index:", idx)
			return buffer.Bytes(), err
		}
	}

	return buffer.Bytes(), nil
}
