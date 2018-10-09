package net.consensys.pantheon.crypto;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PersonalisationString {
  private static final Logger LOGGER = LogManager.getLogger(PersonalisationString.class);
  private static final byte[] NETWORK_MACS = networkHardwareAddresses();

  public static byte[] getPersonalizationString() {
    final Runtime runtime = Runtime.getRuntime();
    final byte[] threadId = Longs.toByteArray(Thread.currentThread().getId());
    final byte[] availProcessors = Ints.toByteArray(runtime.availableProcessors());
    final byte[] freeMem = Longs.toByteArray(runtime.freeMemory());
    final byte[] runtimeMem = Longs.toByteArray(runtime.maxMemory());
    return Bytes.concat(threadId, availProcessors, freeMem, runtimeMem, NETWORK_MACS);
  }

  private static byte[] networkHardwareAddresses() {
    final byte[] networkAddresses = new byte[256];
    final ByteBuffer buffer = ByteBuffer.wrap(networkAddresses);
    try {
      final Enumeration<NetworkInterface> networkInterfaces =
          NetworkInterface.getNetworkInterfaces();
      if (networkInterfaces != null) {
        while (networkInterfaces.hasMoreElements()) {
          final NetworkInterface networkInterface = networkInterfaces.nextElement();
          final byte[] hardwareAddress = networkInterface.getHardwareAddress();
          if (hardwareAddress != null) {
            buffer.put(hardwareAddress);
          }
        }
      }
    } catch (SocketException | BufferOverflowException e) {
      LOGGER.warn(
          "Failed to obtain network hardware address for use in random number personalisation string, "
              + "continuing without this piece of random information",
          e);
    }

    return Arrays.copyOf(networkAddresses, buffer.position());
  }
}