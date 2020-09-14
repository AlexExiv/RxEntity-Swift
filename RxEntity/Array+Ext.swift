//
//  Array+Ext.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 14.09.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

extension Array where Element: REEntity
{
    mutating func AppendOrReplace( entities: [Element] )
    {
        entities.forEach
        {
            e in
            if let i = self.firstIndex( where: { e._key == $0._key } )
            {
                self[i] = e
            }
            else
            {
                self.append( e )
            }
        }
    }
}