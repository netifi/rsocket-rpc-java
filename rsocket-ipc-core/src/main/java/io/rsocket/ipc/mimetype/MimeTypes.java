package io.rsocket.ipc.mimetype;

import io.rsocket.metadata.WellKnownMimeType;

public class MimeTypes {

	public static final MimeType MIME_TYPE_SERVICE = MimeTypes.create(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);
	public static final MimeType MIME_TYPE_METHOD = MimeTypes.create(MIME_TYPE_SERVICE.getString() + "/method");
	public static final MimeType MIME_TYPE_TRACER = MimeTypes.create("message/x.rsocket.ipc.tracer.v0");

	public static MimeType create(String mimeType) {
		return new MimeType.Impl(mimeType);
	}

	public static MimeType create(WellKnownMimeType wellKnownMimeType) {
		return new MimeType.Impl(wellKnownMimeType);
	}

}
