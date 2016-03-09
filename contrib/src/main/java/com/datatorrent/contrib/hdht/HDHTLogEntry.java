/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.contrib.hdht.wal.LogSerializer;
import com.datatorrent.netlet.util.Slice;

/**
 * Class defining data type stored in Write Ahead Log. Also a serializer for them.
 *
 * @since 3.3.0
 */
class HDHTLogEntry
{
  enum HDHTEntryType
  {
    PUT,
    DELETE,
    PURGE;

    private static final Map<Integer, HDHTEntryType> intToTypeMap = new HashMap<Integer, HDHTEntryType>();

    static {
      for (HDHTEntryType type : HDHTEntryType.values()) {
        intToTypeMap.put(type.ordinal(), type);
      }
    }

    public static HDHTEntryType fromInt(int i)
    {
      return intToTypeMap.get(i);
    }
  }

  /**
   * Marker interface for entry in log.
   */
  interface HDHTWalEntry
  {
    int getType();

    long getBucket();

    void writeObject(DataOutputStream dos) throws IOException;

    void readObject(DataInputStream bb) throws IOException;
  }

  /**
   * Data required for Put record in Log.
   */
  static class PutEntry implements HDHTWalEntry
  {
    public byte[] val;
    Slice key;
    long bucketKey;

    public PutEntry(long buckeyKey, Slice key, byte[] val)
    {
      this.key = key;
      this.val = val;
      this.bucketKey = buckeyKey;
    }

    private PutEntry()
    {

    }

    @Override
    public String toString()
    {
      return "PutEntry{" +
        "val=" + Arrays.toString(val) +
        ", key=" + key +
        ", bucketKey=" + bucketKey +
        '}';
    }

    @Override
    public void writeObject(DataOutputStream dos) throws IOException
    {
      dos.writeLong(bucketKey);
      dos.writeInt(key.length);
      dos.write(key.buffer, key.offset, key.length);
      dos.writeInt(val.length);
      dos.write(val);
    }

    @Override
    public void readObject(DataInputStream dis) throws IOException
    {
      bucketKey = dis.readLong();
      int keyLen = dis.readInt();
      byte[] keyBytes = new byte[keyLen];
      dis.readFully(keyBytes);
      key = new Slice(keyBytes);
      int valLen = dis.readInt();
      val = new byte[valLen];
      dis.readFully(val);
    }

    @Override
    public int getType()
    {
      return HDHTEntryType.PUT.ordinal();
    }

    @Override
    public long getBucket()
    {
      return bucketKey;
    }
  }

  /**
   * Data required for Delete record in Log.
   */
  static class DeleteEntry implements HDHTWalEntry
  {
    Slice key;
    long bucketKey;

    public DeleteEntry(long bucketKey, Slice key)
    {
      this.key = key;
      this.bucketKey = bucketKey;
    }

    private DeleteEntry()
    {

    }

    @Override
    public String toString()
    {
      return "DeleteEntry{" +
        "key=" + key +
        ", bucketKey=" + bucketKey +
        '}';
    }

    @Override
    public void writeObject(DataOutputStream dos) throws IOException
    {
      dos.writeLong(bucketKey);
      dos.writeInt(key.length);
      dos.write(key.buffer, key.offset, key.length);
    }

    @Override
    public void readObject(DataInputStream dis) throws IOException
    {
      bucketKey = dis.readLong();
      int keyLen = dis.readInt();
      byte[] bytes = new byte[keyLen];
      dis.readFully(bytes);
      key = new Slice(bytes);
    }

    @Override
    public int getType()
    {
      return HDHTEntryType.DELETE.ordinal();
    }

    @Override
    public long getBucket()
    {
      return bucketKey;
    }
  }

  /**
   * Data required for Purge record in Log.
   */
  static class PurgeEntry implements HDHTWalEntry
  {
    public Slice startKey;
    public Slice endKey;
    public long bucketKey;

    public PurgeEntry(long bucketKey, Slice startKey, Slice endKey)
    {
      this.startKey = startKey;
      this.endKey = endKey;
      this.bucketKey = bucketKey;
    }

    private PurgeEntry()
    {

    }

    @Override
    public String toString()
    {
      return "PurgeEntry{" +
        "startKey=" + startKey +
        ", endKey=" + endKey +
        '}';
    }

    @Override
    public void writeObject(DataOutputStream dos) throws IOException
    {
      dos.writeLong(bucketKey);
      dos.writeInt(startKey.length);
      dos.write(startKey.buffer, startKey.offset, startKey.length);
      dos.writeInt(endKey.length);
      dos.write(endKey.buffer, endKey.offset, endKey.length);
    }

    @Override
    public void readObject(DataInputStream dis) throws IOException
    {
      bucketKey = dis.readLong();
      int startKeyLen = dis.readInt();
      byte[] startKeyBytes = new byte[startKeyLen];
      dis.readFully(startKeyBytes);
      startKey = new Slice(startKeyBytes);
      int endKeyLen = dis.readInt();
      byte[] endKeyBytes = new byte[endKeyLen];
      dis.readFully(endKeyBytes);
      endKey = new Slice(endKeyBytes);
    }

    @Override
    public int getType()
    {
      return HDHTEntryType.PURGE.ordinal();
    }

    @Override
    public long getBucket()
    {
      return bucketKey;
    }
  }

  /**
   * Serializer for HDHT Log records.
   */
  static class HDHTLogSerializer implements LogSerializer<HDHTWalEntry>
  {
    static Map<Integer, Class<? extends HDHTWalEntry>> entryMap = Maps.newHashMap();

    static {
      entryMap.put(HDHTEntryType.PUT.ordinal(), PutEntry.class);
      entryMap.put(HDHTEntryType.DELETE.ordinal(), DeleteEntry.class);
      entryMap.put(HDHTEntryType.PURGE.ordinal(), PurgeEntry.class);
    }

    @Override
    public Slice fromObject(HDHTWalEntry entry)
    {
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bao);
      try {
        dos.writeInt(entry.getType());
        entry.writeObject(dos);
        dos.flush();
      } catch (IOException ex) {
        throw new RuntimeException("Unable to serialize WAL entry", ex);
      }
      return new Slice(bao.toByteArray());
    }

    @Override
    public HDHTWalEntry toObject(Slice s)
    {
      ByteArrayInputStream is = new ByteArrayInputStream(s.buffer, s.offset, s.length);
      DataInputStream dis = new DataInputStream(is);
      try {
        HDHTEntryType type = HDHTEntryType.fromInt(dis.readInt());
        HDHTWalEntry entry = null;
        switch (type) {
          case PUT:
            entry = new PutEntry();
            break;
          case DELETE:
            entry = new DeleteEntry();
            break;
          case PURGE:
            entry = new PurgeEntry();
            break;
          default:
        }
        entry.readObject(dis);
        return entry;
      } catch (IOException ex) {
        throw new RuntimeException("Unable to convert to object", ex);
      }
    }
  }
}
