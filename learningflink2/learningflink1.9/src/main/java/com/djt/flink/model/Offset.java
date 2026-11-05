package com.djt.flink.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author dajiangtai
 * @create 2020-01-16-17:13
 */
@Data
@ToString
public class Offset  implements Serializable {
    private String rowkey;
    private String p0;
    private String p1;
    private String p2;
}
