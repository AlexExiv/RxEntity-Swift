//
//  REEntityCollectionConfig.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 11.08.2025.
//  Copyright © 2025 ALEXEY ABDULIN. All rights reserved.
//

public class REEntityCollectionConfig
{
    static public var isLogging = false
    
    static public func Log( _ items: Any..., separator: String = " ", terminator: String = "\n" )
    {
        if isLogging
        {
            print( items, separator: separator, terminator: terminator )
        }
    }
}

