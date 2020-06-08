/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.cassandraoutput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;

/**
 * Provides a popup dialog for editing CQL commands.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class EnterCQLDialog extends Dialog {

  private static Class<?> PKG = EnterCQLDialog.class; // for i18n purposes,
                                                      // needed by Translator2!!
                                                      // $NON-NLS-1$

  protected String dialogTitle;

  protected String originalCql;
  protected String currentCql;

  protected Shell dialogParent;
  protected Shell dialogShell;

  protected Button wbOk;
  protected Button wbCancel;
  protected Listener lsCancel;
  protected Listener lsOk;

  protected Button wbDontComplainAboutAprioriCqlFailing;

  protected PropsUI dialogProps;

  protected StyledTextComp wCqlText;

  protected TransMeta transMeta;

  protected ModifyListener lsMod;

  protected boolean dontComplain;

  public EnterCQLDialog( Shell parent, TransMeta transMeta, ModifyListener lsMod, String title, String cql,
      boolean dontComplain ) {
    super( parent, SWT.NONE );

    dialogParent = parent;
    dialogProps = PropsUI.getInstance();
    dialogTitle = title;
    originalCql = cql;
    this.transMeta = transMeta;
    this.lsMod = lsMod;
    this.dontComplain = dontComplain;
  }

  public String open() {

    Display display = dialogParent.getDisplay();

    dialogShell = new Shell( dialogParent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
    dialogProps.setLook( dialogShell );
    dialogShell.setImage( GUIResource.getInstance().getImageSpoon() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    dialogShell.setLayout( formLayout );
    dialogShell.setText( dialogTitle );

    int margin = Const.MARGIN;
    int middle = Const.MIDDLE_PCT;

    Label dontComplainLab = new Label( dialogShell, SWT.RIGHT );
    dialogProps.setLook( dontComplainLab );
    dontComplainLab.setText( BaseMessages.getString( this.getClass(), "EnterCQLDialog.DontComplainIfCQLFails.Label" ) ); //$NON-NLS-1$
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.bottom = new FormAttachment( 100, -50 );
    dontComplainLab.setLayoutData( fd );

    wbDontComplainAboutAprioriCqlFailing = new Button( dialogShell, SWT.CHECK );
    dialogProps.setLook( wbDontComplainAboutAprioriCqlFailing );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    fd.left = new FormAttachment( middle, 0 );
    wbDontComplainAboutAprioriCqlFailing.setLayoutData( fd );
    wbDontComplainAboutAprioriCqlFailing.setSelection( dontComplain );
    wbDontComplainAboutAprioriCqlFailing.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        dontComplain = wbDontComplainAboutAprioriCqlFailing.getSelection();
      }
    } );

    wCqlText =
        new StyledTextComp( transMeta, dialogShell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" ); //$NON-NLS-1$
    dialogProps.setLook( wCqlText, dialogProps.WIDGET_STYLE_FIXED );

    wCqlText.setText( originalCql );
    currentCql = originalCql;

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, -2 * margin );
    fd.bottom = new FormAttachment( wbDontComplainAboutAprioriCqlFailing, -margin );
    wCqlText.setLayoutData( fd );
    wCqlText.addModifyListener( lsMod );
    wCqlText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wCqlText.setToolTipText( transMeta.environmentSubstitute( wCqlText.getText() ) );
      }
    } );

    // Text Highlighting
    wCqlText.addLineStyleListener( new SQLValuesHighlight() );

    // Some buttons
    wbOk = new Button( dialogShell, SWT.PUSH );
    wbOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    wbCancel = new Button( dialogShell, SWT.PUSH );
    wbCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    BaseStepDialog.positionBottomButtons( dialogShell, new Button[] {wbOk, wbCancel}, margin, null );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wbOk.addListener( SWT.Selection, lsOk );
    wbCancel.addListener( SWT.Selection, lsCancel );

    // Detect [X] or ALT-F4 or something that kills this window...
    dialogShell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        checkCancel( e );
      }
    } );

    BaseStepDialog.setSize( dialogShell );
    dialogShell.open();

    while ( !dialogShell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return currentCql;
  }

  public void dispose() {
    dialogProps.setScreen( new WindowProperty( dialogShell ) );
    dialogShell.dispose();
  }

  public boolean getDontComplainStatus() {
    return dontComplain;
  }

  protected void ok() {
    currentCql = wCqlText.getText();
    dispose();
  }

  protected void cancel() {
    currentCql = originalCql;
    dispose();
  }

  public void checkCancel( ShellEvent e ) {
    String newText = wCqlText.getText();
    if ( !newText.equals( originalCql ) ) {
      int save = JobGraph.showChangedWarning( dialogShell, dialogTitle );
      if ( save == SWT.CANCEL ) {
        e.doit = false;
      } else if ( save == SWT.YES ) {
        ok();
      } else {
        cancel();
      }
    } else {
      cancel();
    }
  }
}
