package io.rsocket.ipc.mimetype;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.rsocket.ipc.util.IPCUtils;
import io.rsocket.metadata.WellKnownMimeType;

public interface MimeType {

	String getString();

	Optional<WellKnownMimeType> getWellKnownMimeType();

	static class Impl implements MimeType {

		private final String mimeTypeFallback;
		private final Supplier<WellKnownMimeType> wellKnownMimeTypeSupplier;

		public Impl(WellKnownMimeType wellKnownMimeType) {
			Objects.requireNonNull(wellKnownMimeType);
			this.wellKnownMimeTypeSupplier = () -> wellKnownMimeType;
			this.mimeTypeFallback = null;
		}

		public Impl(String mimeType) {
			IPCUtils.requireNonEmpty(mimeType);
			this.wellKnownMimeTypeSupplier = new Supplier<WellKnownMimeType>() {

				private Optional<WellKnownMimeType> parsed;

				@Override
				public WellKnownMimeType get() {
					if (parsed == null)
						synchronized (this) {
							if (parsed == null)
								parsed = IPCUtils.parseWellKnownMimeType(mimeType);
						}
					return parsed.orElse(null);
				}
			};
			this.mimeTypeFallback = mimeType;
		}

		@Override
		public String getString() {
			Optional<WellKnownMimeType> wkmtOp = getWellKnownMimeType();
			if (wkmtOp.isPresent())
				return wkmtOp.get().getString();
			// shouldn't happen, but maybe some weird overriding
			return IPCUtils.requireNonEmpty(mimeTypeFallback);
		}

		@Override
		public Optional<WellKnownMimeType> getWellKnownMimeType() {
			return Optional.ofNullable(wellKnownMimeTypeSupplier.get());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			String strValue = getString().toLowerCase();
			result = prime * result + ((strValue == null) ? 0 : strValue.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof MimeType))
				return false;
			String strValue = this.getString().toLowerCase();
			String strValueOther = ((MimeType) obj).getString();
			if (strValueOther != null)
				strValueOther = strValueOther.toLowerCase();
			return Objects.equals(strValue, strValueOther);
		}

		@Override
		public String toString() {
			WellKnownMimeType wellKnownMimeType = getWellKnownMimeType().orElse(null);
			String result = "Impl [wellKnownMimeType=" + wellKnownMimeType + ", mimeTypeFallback=" + mimeTypeFallback
					+ "]";
			return result;
		}

	}

}
