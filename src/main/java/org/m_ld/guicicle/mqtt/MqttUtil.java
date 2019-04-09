/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

public interface MqttUtil
{
    static boolean isMatch(String topicName, String topicPattern)
    {
        boolean canWildcard = true;
        for (int i = 0, j = 0; i < topicPattern.length() && j < topicName.length(); i++, j++)
        {
            if (canWildcard && topicPattern.charAt(i) == '#')
            {
                return true;
            }
            else if (canWildcard && topicPattern.charAt(i) == '+')
            {
                if (topicName.charAt(j) == '/')
                {
                    if (i == topicPattern.length() - 1)
                        return false;
                    j--; // Topic pattern will advance
                }
                else
                    i--; // Topic name will advance
            }
            else if (topicPattern.charAt(i) != topicName.charAt(j))
            {
                return false;
            }
            canWildcard = i < 0 || (topicPattern.charAt(i) == '/');
        }
        return true;
    }
}
