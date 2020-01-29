package org.apache.carbondata.core.transaction;

import java.util.Locale;

public class TransactionFailedException extends Exception {
  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The Error message.
   */
  private String msg = "";

  /**
   * Constructor
   *
   * @param msg The error message for this exception.
   */
  public TransactionFailedException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param msg       exception message
   * @param throwable detail exception
   */
  public TransactionFailedException(String msg, Throwable throwable) {
    super(msg, throwable);
    this.msg = msg;
  }

  /**
   * Constructor
   *
   * @param throwable exception
   */
  public TransactionFailedException(Throwable throwable) {
    super(throwable);
  }

  /**
   * This method is used to get the localized message.
   *
   * @param locale - A Locale object represents a specific geographical,
   *               political, or cultural region.
   * @return - Localized error message.
   */
  public String getLocalizedMessage(Locale locale) {
    return "";
  }

  /**
   * getLocalizedMessage
   */
  @Override public String getLocalizedMessage() {
    return super.getLocalizedMessage();
  }

  /**
   * getMessage
   */
  public String getMessage() {
    return this.msg;
  }
}

