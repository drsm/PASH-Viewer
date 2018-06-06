/*
 *-------------------
 * The DbConnection.java is part of ASH Viewer
 *-------------------
 * 
 * ASH Viewer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ASH Viewer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ASH Viewer.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Copyright (c) 2009, Alex Kardapolov, All rights reserved.
 *
 */
package org.ash.conn.settings;

// for test
import java.awt.*;
import javax.swing.*;
import java.awt.event.*;
// for test

/**
 * The Class DbConnection.
 */
public class DbConnection {
  
  /** The name. */
  private String name;
  
  /** The class name. */
  private String className;
  
  /** The url. */
  private String url;
  
  /** The username. */
  private String username;
  
  /** The password. */
  private String password;
  
  /**
   * Instantiates a new db connection.
   * 
   * @param name the name
   * @param className the class name
   * @param url the url
   * @param username the username
   * @param password the password
   */
  public DbConnection(
      String name,
      String className,
      String url,
      String username,
      String password) {
    this.name = name;
    this.className = className;
    this.url = url;
    this.username = username;
    this.password = password;
  }
  
  /**
   * Gets the class name of PostgreSQL Driver.
   * 
   * @param dbType the db type
   * 
   * @return the class name
   */
  public String getClassNameDriverName() {
	return "org.postgresql.Driver";
  }

  /**
   * Gets the url.
   * 
   * @param host the host
   * @param port the port
   *
   * @return the url
   */
  public String getUrl(String host,String port, String db) {
	return "jdbc:postgresql://"+host+":"+port+"/"+db;
  }

  /**
   * Gets the host.
   * 
   * @return the host
   */
  public String getHost() {
	return url.substring(18,url.indexOf(":",18));
  }

  /**
   * Gets the port.
   * 
   * @return the port
   */
  public String getPort() {
    int index = url.indexOf(":",18);
      return url.substring(index+1,url.indexOf("/",index+1));
  }

  public String getDB() {
      int index = url.indexOf("/",18) + 1;
      return url.substring(index);

  }


  /**
   * Gets the class name.
   * 
   * @return the class name
   */
  public String getClassName() {
    return className;
  }

  /**
   * Gets the name.
   * 
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the url.
   * 
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Gets the user name.
   * 
   * @return the user name
   */
  public String getUsername() {
    return username;
  }
  
  /**
   * Gets the password.
   * 
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the class name.
   * 
   * @param className the new class name
   */
  public void setClassName(String className) {
    this.className = className;
  }

  /**
   * Sets the profile name.
   * 
   * @param name the new profile name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets the password.
   * 
   * @param password the new password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Sets the url.
   * 
   * @param url the new url
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Sets the user name.
   * 
   * @param username the new user name
   */
  public void setUsername(String username) {
    this.username = username;
  }
  
}