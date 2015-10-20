/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.contrib.hdht.log.LogSerializer;
import com.datatorrent.netlet.util.Slice;

/**
 * Class defining data type stored in Write Ahead Log. Also a serializer for them.
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

  ;

  /**
   * Marker interface for entry in log.
   */
  interface HDHTWalEntry
  {
  }

  /**
   * Data required for Put record in Log.
   */
  static class PutEntry implements HDHTWalEntry
  {
    public byte[] val;
    Slice key;

    public PutEntry(Slice key, byte[] val)
    {
      this.key = key;
      this.val = val;
    }

    @Override
    public String toString()
    {
      return "PutEntry{" +
        "val=" + Arrays.toString(val) +
        ", key=" + key +
        '}';
    }
  }

  /**
   * Data required for Delete record in Log.
   */
  static class DeleteEntry implements HDHTWalEntry
  {
    Slice key;

    public DeleteEntry(Slice key)
    {
      this.key = key;
    }

    @Override
    public String toString()
    {
      return "DeleteEntry{" +
        "key=" + key +
        '}';
    }
  }

  /**
   * Data required for Purge record in Log.
   */
  static class PurgeEntry implements HDHTWalEntry
  {
    public Slice startKey;
    public Slice endKey;

    public PurgeEntry(Slice startKey, Slice endKey)
    {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    @Override
    public String toString()
    {
      return "PurgeEntry{" +
        "startKey=" + startKey +
        ", endKey=" + endKey +
        '}';
    }
  }

  /**
   * Serializer for HDHT Log records.
   */
  static class HDHTLogSerializer implements LogSerializer<HDHTWalEntry>
  {

    static final int TUPLE_TYPE_SIZE = 4;
    static Map<Integer, Class<? extends HDHTWalEntry>> entryMap = Maps.newHashMap();

    static {
      entryMap.put(HDHTEntryType.PUT.ordinal(), PutEntry.class);
      entryMap.put(HDHTEntryType.DELETE.ordinal(), DeleteEntry.class);
      entryMap.put(HDHTEntryType.PURGE.ordinal(), PurgeEntry.class);
    }

    @Override
    public Slice fromObject(HDHTWalEntry entry)
    {
      ByteBuffer buffer;
      int size = TUPLE_TYPE_SIZE;

      if (entry instanceof PutEntry) {
        size += 4 + ((PutEntry)entry).key.length + 4 + ((PutEntry)entry).val.length;
        buffer = ByteBuffer.allocate(size);
        buffer.putInt(HDHTEntryType.PUT.ordinal());
        Slice key = ((PutEntry)entry).key;
        buffer.putInt(key.length);
        buffer.put(key.buffer, key.offset, key.length);
        buffer.putInt(((PutEntry)entry).val.length);
        buffer.put(((PutEntry)entry).val);
      } else if (entry instanceof DeleteEntry) {
        size += 4 + ((DeleteEntry)entry).key.length;
        buffer = ByteBuffer.allocate(size);
        buffer.putInt(HDHTEntryType.DELETE.ordinal());
        buffer.putInt(((DeleteEntry)entry).key.length);
        Slice key = ((DeleteEntry)entry).key;
        buffer.put(key.buffer, key.offset, key.length);
      } else if (entry instanceof PurgeEntry) {
        size += 4 + ((PurgeEntry)entry).startKey.length + 4 + ((PurgeEntry)entry).endKey.length;
        buffer = ByteBuffer.allocate(size);
        buffer.putInt(HDHTEntryType.PURGE.ordinal());
        buffer.putInt(((PurgeEntry)entry).startKey.length);
        buffer.put(((PurgeEntry)entry).startKey.buffer, ((PurgeEntry)entry).startKey.offset, ((PurgeEntry)entry).startKey.length);
        buffer.putInt(((PurgeEntry)entry).endKey.length);
        buffer.put(((PurgeEntry)entry).endKey.buffer, ((PurgeEntry)entry).endKey.offset, ((PurgeEntry)entry).endKey.length);
      } else {
        throw new RuntimeException("Not valid log entry type");
      }
      return new Slice(buffer.array());
    }

    @Override
    public HDHTWalEntry toObject(Slice s)
    {
      ByteBuffer buffer = ByteBuffer.wrap(s.buffer, s.offset, s.length);
      int type = buffer.getInt();
      HDHTEntryType entry = HDHTEntryType.fromInt(type);
      switch (entry) {
        case PUT:
          int keyLen = buffer.getInt();
          byte[] key = new byte[keyLen];
          buffer.get(key);
          int valLen = buffer.getInt();
          byte[] val = new byte[valLen];
          buffer.get(val);
          return new PutEntry(new Slice(key), val);
        case DELETE:
          keyLen = buffer.getInt();
          key = new byte[keyLen];
          buffer.get(key);
          return new DeleteEntry(new Slice(key));
        case PURGE:
          int startLen = buffer.getInt();
          byte[] start = new byte[startLen];
          buffer.get(start);
          int endLen = buffer.getInt();
          byte[] end = new byte[endLen];
          buffer.get(end);
          return new PurgeEntry(new Slice(start), new Slice(end));
      }
      throw new RuntimeException("Invalid entry type");
    }
  }
}
