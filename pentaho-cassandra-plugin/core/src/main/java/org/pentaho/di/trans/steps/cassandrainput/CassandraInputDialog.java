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

package org.pentaho.di.trans.steps.cassandrainput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.spi.IQueryMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.events.dialog.FilterType;
import org.pentaho.di.ui.core.events.dialog.ProviderFilterType;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;

/**
 * Dialog class for the CassandraInput step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = CassandraInputMeta.class;

  private final CassandraInputMeta currentMeta;
  private final CassandraInputMeta originalMeta;

  private TextVar wHost;
  private TextVar wPort;

  private TextVar wUser;
  private TextVar wPassword;

  private TextVar wKeyspace;

  private Button wCompression;

  private Button wSsl;

  private TextVar wTimeout;

  private TextVar wReadTimeout;

  private TextVar wRowFetchSize;

  private Label wlPosition;

  private Button wbShowSchema;

  private StyledTextComp wCql;

  private TextVar wLocalDataCenter;

  private TextVar wApplicationConf;
  private Button wbbApplicationConf;

  private Button wExecuteForEachRow;

  private Button wExpandMaps;
  private Button wExpandCollection;

  public CassandraInputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );

    currentMeta = (CassandraInputMeta) in;
    originalMeta = (CassandraInputMeta) currentMeta.clone();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, currentMeta );

    // used to listen to a text field (m_wStepname)
    ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        currentMeta.setChanged();
      }
    };

    SelectionListener lsSel = new SelectionListener() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        lsMod.modifyText( null );
      }

      @Override
      public void widgetDefaultSelected( SelectionEvent selectionEvent ) {

      }
    };

    changed = currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Shell.Title" ) ); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "CassandraInputDialog.StepName.Label" ) ); //$NON-NLS-1$
    props.setLook( wlStepname );

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fd );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fd );


    // Application conf
    Label wlApplicationConf = new Label( shell, SWT.RIGHT );
    props.setLook( wlApplicationConf );
    wlApplicationConf.setText( BaseMessages.getString( PKG, "CassandraInputDialog.ApplicationConf.Label" ) );
    wlApplicationConf.setLayoutData( new FormDataBuilder().left( 0, 0 ).top( wStepname, margin ).right( middle, -margin ).result() );

    wbbApplicationConf = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbApplicationConf );
    wbbApplicationConf.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbApplicationConf.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    wbbApplicationConf.setLayoutData( new FormDataBuilder().top( wStepname, margin ).right( 100, 0 ).result() );

    wApplicationConf = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wApplicationConf );
    wApplicationConf.addModifyListener( lsMod );
    wApplicationConf.setLayoutData( new FormDataBuilder().left( middle, 0 ).top( wStepname, margin ).right( wbbApplicationConf, -margin ).result() );

    wbbApplicationConf.addSelectionListener( new SelectionAdapterFileDialogTextVar( log, wApplicationConf, transMeta,
      new SelectionAdapterOptions( SelectionOperation.FILE,
        new String[] { FilterType.CONF.toString(), FilterType.JSON.toString(), "properties",
          FilterType.ALL.toString() }, FilterType.CONF.toString(),
        new String[] { ProviderFilterType.LOCAL.toString() }, false ) ) );

    wApplicationConf.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent modifyEvent ) {
        updateApplicationConf();
      }
    } );
    // host line
    /**
     * various UI bits and pieces for the dialog
     */
    Label wlHost = new Label( shell, SWT.RIGHT );
    props.setLook( wlHost );
    wlHost.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Hostname.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wbbApplicationConf, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlHost.setLayoutData( fd );

    wHost = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHost );
    wHost.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wHost.setToolTipText( transMeta.environmentSubstitute( wHost.getText() ) );
      }
    } );
    wHost.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wbbApplicationConf, margin );
    fd.left = new FormAttachment( middle, 0 );
    wHost.setLayoutData( fd );

    // port line
    Label wlPort = new Label( shell, SWT.RIGHT );
    props.setLook( wlPort );
    wlPort.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Port.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wHost, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fd );

    wPort = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wPort.setToolTipText( transMeta.environmentSubstitute( wPort.getText() ) );
      }
    } );
    wPort.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wHost, margin );
    fd.left = new FormAttachment( middle, 0 );
    wPort.setLayoutData( fd );

    // keyspace line
    Label wlLocalDataCenter = new Label( shell, SWT.RIGHT );
    props.setLook( wlLocalDataCenter );
    wlLocalDataCenter.setText( BaseMessages.getString( PKG, "CassandraInputDialog.LocalDataCenter.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wPort, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlLocalDataCenter.setLayoutData( fd );

    wLocalDataCenter = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLocalDataCenter );
    wLocalDataCenter.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wLocalDataCenter.setToolTipText( transMeta.environmentSubstitute( wLocalDataCenter.getText() ) );
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wPort, margin );
    fd.left = new FormAttachment( middle, 0 );
    wLocalDataCenter.setLayoutData( fd );

    Label lSsl = new Label( shell, SWT.RIGHT );
    props.setLook( lSsl );
    lSsl.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Ssl.Label" ) );
    lSsl.setLayoutData(
      new FormDataBuilder().left( 0, 0 ).top( wLocalDataCenter, margin ).right( middle, -margin ).result() );

    wSsl = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wSsl );
    wSsl
        .setLayoutData( new FormDataBuilder().left( middle, 0 ).right( 100, 0 ).top( wLocalDataCenter, margin ).result() );

    // timeout line
    Label wlTimeout = new Label( shell, SWT.RIGHT );
    props.setLook( wlTimeout );
    wlTimeout.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Timeout.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSsl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlTimeout.setLayoutData( fd );

    wTimeout = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTimeout );
    wTimeout.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wTimeout.setToolTipText( transMeta.environmentSubstitute( wTimeout.getText() ) );
      }
    } );
    wTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSsl, margin );
    fd.left = new FormAttachment( middle, 0 );
    wTimeout.setLayoutData( fd );

    // read timeout
    Label wlReadTimeout = new Label( shell, SWT.RIGHT );
    props.setLook( wlReadTimeout );
    wlReadTimeout
        .setText( BaseMessages.getString( PKG, "CassandraInputDialog.ReadTimeout.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlReadTimeout.setLayoutData( fd );

    wReadTimeout = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReadTimeout );
    wReadTimeout.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wReadTimeout.setToolTipText( transMeta.environmentSubstitute( wReadTimeout.getText() ) );
      }
    } );
    wReadTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wTimeout, margin );
    fd.left = new FormAttachment( middle, 0 );
    wReadTimeout.setLayoutData( fd );

    // row fetch size timeout
    Label wlRowFetchSize = new Label( shell, SWT.RIGHT );
    props.setLook( wlRowFetchSize );
    wlRowFetchSize
        .setText( BaseMessages.getString( PKG, "CassandraInputDialog.RowFetchSize.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wReadTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlRowFetchSize.setLayoutData( fd );

    wRowFetchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowFetchSize );
    wRowFetchSize.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wRowFetchSize.setToolTipText( transMeta.environmentSubstitute( wRowFetchSize.getText() ) );
      }
    } );
    wRowFetchSize.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wReadTimeout, margin );
    fd.left = new FormAttachment( middle, 0 );
    wRowFetchSize.setLayoutData( fd );

    // username line
    Label wlUser = new Label( shell, SWT.RIGHT );
    props.setLook( wlUser );
    wlUser.setText( BaseMessages.getString( PKG, "CassandraInputDialog.User.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wRowFetchSize, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlUser.setLayoutData( fd );

    wUser = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUser );
    wUser.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wRowFetchSize, margin );
    fd.left = new FormAttachment( middle, 0 );
    wUser.setLayoutData( fd );

    // password line
    Label wlPassword = new Label( shell, SWT.RIGHT );
    props.setLook( wlPassword );
    wlPassword.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Password.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wUser, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fd );

    wPassword = new PasswordTextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );

    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wUser, margin );
    fd.left = new FormAttachment( middle, 0 );
    wPassword.setLayoutData( fd );

    // keyspace line
    Label wlKeyspace = new Label( shell, SWT.RIGHT );
    props.setLook( wlKeyspace );
    wlKeyspace.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Keyspace.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wPassword, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlKeyspace.setLayoutData( fd );

    wKeyspace = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyspace );
    wKeyspace.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wKeyspace.setToolTipText( transMeta.environmentSubstitute( wKeyspace.getText() ) );
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wPassword, margin );
    fd.left = new FormAttachment( middle, 0 );
    wKeyspace.setLayoutData( fd );



    // compression check box
    Label wlCompression = new Label( shell, SWT.RIGHT );
    props.setLook( wlCompression );
    wlCompression
        .setText( BaseMessages.getString( PKG, "CassandraInputDialog.UseCompression.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wKeyspace, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlCompression.setLayoutData( fd );

    wCompression = new Button( shell, SWT.CHECK );
    props.setLook( wCompression );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( wKeyspace, margin );
    wCompression.setLayoutData( fd );
    wCompression.addSelectionListener( lsSel );

    // execute for each row
    Label executeForEachLab = new Label( shell, SWT.RIGHT );
    props.setLook( executeForEachLab );
    executeForEachLab
        .setText( BaseMessages.getString( PKG, "CassandraInputDialog.ExecuteForEachRow.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( middle, -margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wCompression, margin );
    executeForEachLab.setLayoutData( fd );

    wExecuteForEachRow = new Button( shell, SWT.CHECK );
    props.setLook( wExecuteForEachRow );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( wCompression, margin );
    wExecuteForEachRow.setLayoutData( fd );
    wExecuteForEachRow.addSelectionListener( lsSel );

    // expand collection
    Label expandCollectionLabel = new Label( shell, SWT.RIGHT );
    props.setLook( expandCollectionLabel );
    expandCollectionLabel
      .setText( BaseMessages.getString( PKG, "CassandraInputDialog.ExpandCollection.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( middle, -margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wExecuteForEachRow, margin );
    expandCollectionLabel.setLayoutData( fd );

    wExpandCollection = new Button( shell, SWT.CHECK );
    props.setLook( wExpandCollection );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( wExecuteForEachRow, margin );
    wExpandCollection.setLayoutData( fd );
    wExpandCollection.addSelectionListener( lsSel );

    // execute for each row
    Label expandMapsLab = new Label( shell, SWT.RIGHT );
    props.setLook( expandMapsLab );
    expandMapsLab
      .setText( BaseMessages.getString( PKG, "CassandraInputDialog.ExpandMaps.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( middle, -margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wExpandCollection, margin );
    expandMapsLab.setLayoutData( fd );

    wExpandMaps = new Button( shell, SWT.CHECK );
    props.setLook( wExpandMaps );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( wExpandCollection, margin );
    wExpandMaps.setLayoutData( fd );
    wExpandMaps.addSelectionListener( lsSel );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) ); //$NON-NLS-1$

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    setButtonPositions( new Button[] {
      wOK, wPreview, wCancel }, margin, wCql );

    // position label
    wlPosition = new Label( shell, SWT.NONE );
    props.setLook( wlPosition );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.bottom = new FormAttachment( wOK, -margin );
    wlPosition.setLayoutData( fd );

    wbShowSchema = new Button( shell, SWT.PUSH );
    wbShowSchema.setText( BaseMessages.getString( PKG, "CassandraInputDialog.Schema.Button" ) ); //$NON-NLS-1$
    props.setLook( wbShowSchema );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( wOK, -margin );
    wbShowSchema.setLayoutData( fd );

    wbShowSchema.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        popupSchemaInfo();
      }
    } );

    // cql stuff
    Label wlCql = new Label( shell, SWT.NONE );
    props.setLook( wlCql );
    wlCql.setText( BaseMessages.getString( PKG, "CassandraInputDialog.CQL.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wExpandMaps, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlCql.setLayoutData( fd );

    wCql =
        new StyledTextComp( transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
            "" ); //$NON-NLS-1$
    props.setLook( wCql, PropsUI.WIDGET_STYLE_FIXED );
    wCql.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wlCql, margin );
    fd.right = new FormAttachment( 100, -2 * margin );
    fd.bottom = new FormAttachment( wbShowSchema, -margin );
    wCql.setLayoutData( fd );
    wCql.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        setPosition();
        wCql.setToolTipText( transMeta.environmentSubstitute( wCql.getText() ) );
      }
    } );

    // Text Highlighting
    wCql.addLineStyleListener( new SQLValuesHighlight() );

    wCql.addKeyListener( new KeyAdapter() {
      @Override
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      @Override
      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );

    wCql.addFocusListener( new FocusAdapter() {
      @Override
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      @Override
      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );

    wCql.addMouseListener( new MouseAdapter() {
      @Override
      public void mouseDoubleClick( MouseEvent e ) {
        setPosition();
      }

      @Override
      public void mouseDown( MouseEvent e ) {
        setPosition();
      }

      @Override
      public void mouseUp( MouseEvent e ) {
        setPosition();
      }
    } );

    // Add listeners
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOK = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsPreview = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        preview();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    wPreview.addListener( SWT.Selection, lsPreview );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    setSize();

    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }

  private void updateApplicationConf() {
    boolean usesConf = !Utils.isEmpty( wApplicationConf.getText() );
    wSsl.setEnabled( !usesConf );
    wTimeout.setEnabled( !usesConf );
    wReadTimeout.setEnabled( !usesConf );
    wRowFetchSize.setEnabled( !usesConf );
  }

  private void getInfo( CassandraInputMeta meta ) {
    meta.setCassandraHost( wHost.getText() );
    meta.setCassandraPort( wPort.getText() );
    meta.setSocketTimeout( wTimeout.getText() );
    meta.setReadTimeout( wReadTimeout.getText() );
    meta.setRowFetchSize( wRowFetchSize.getText() );
    meta.setUsername( wUser.getText() );
    meta.setPassword( wPassword.getText() );
    meta.setCassandraKeyspace( wKeyspace.getText() );
    meta.setLocalDataCenter( wLocalDataCenter.getText() );
    meta.setUseCompression( wCompression.getSelection() );
    meta.setCQLSelectQuery( wCql.getText() );
    meta.setExecuteForEachIncomingRow( wExecuteForEachRow.getSelection() );
    meta.setSsl( wSsl.getSelection() );
    meta.setApplicationConfFile( wApplicationConf.getText() );
    meta.setNotExpandingMaps( !wExpandMaps.getSelection() );
    meta.setExpandComplex( wExpandCollection.getSelection() );
  }

  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText();
    getInfo( currentMeta );

    if ( !originalMeta.equals( currentMeta ) ) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  protected void cancel() {
    stepname = null;
    currentMeta.setChanged( changed );

    dispose();
  }

  protected void getData() {
    wHost.setText( Const.nullToEmpty( currentMeta.getCassandraHost() ) );

    wPort.setText( Const.nullToEmpty( currentMeta.getCassandraPort() ) );

    wTimeout.setText( Const.nullToEmpty( currentMeta.getSocketTimeout() ) );

    wReadTimeout.setText( Const.nullToEmpty( currentMeta.getReadTimeout() ) );

    wRowFetchSize.setText( Const.NVL( currentMeta.getRowFetchSize(), "" ) );

    wUser.setText( Const.nullToEmpty( currentMeta.getUsername() ) );

    wPassword.setText( Const.nullToEmpty( currentMeta.getPassword() ) );

    wKeyspace.setText( Const.nullToEmpty( currentMeta.getCassandraKeyspace() ) );

    wLocalDataCenter.setText( Const.NVL( currentMeta.getLocalDataCenter(), "" ) );

    wCompression.setSelection( currentMeta.getUseCompression() );
    wExecuteForEachRow.setSelection( currentMeta.getExecuteForEachIncomingRow() );
    wSsl.setSelection( currentMeta.isSsl() );
    wApplicationConf.setText( Const.nullToEmpty( currentMeta.getApplicationConfFile() ) );

    if ( !Utils.isEmpty( currentMeta.getCQLSelectQuery() ) ) {
      wCql.setText( currentMeta.getCQLSelectQuery() );
    }

    wExpandMaps.setSelection( !currentMeta.isNotExpandingMaps() );
    wExpandCollection.setSelection( currentMeta.isExpandComplex() );
  }

  protected void setPosition() {
    String scr = wCql.getText();
    int linenr = wCql.getLineAtOffset( wCql.getCaretOffset() ) + 1;
    int posnr = wCql.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }
    wlPosition
        .setText( BaseMessages.getString( PKG, "CassandraInputDialog.Position.Label", "" + linenr,
          "" + colnr ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
  }

  private boolean checkForUnresolved( CassandraInputMeta meta, String title ) {
    String query = transMeta.environmentSubstitute( meta.getCQLSelectQuery() );

    boolean notOk = ( query.contains( "${" ) || query.contains( "?{" ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( notOk ) {
      ShowMessageDialog smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK, title, BaseMessages.getString( PKG,
            "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs" ) ); //$NON-NLS-1$
      smd.open();
    }

    return !notOk;
  }

  private void preview() {
    CassandraInputMeta oneMeta = new CassandraInputMeta();
    getInfo( oneMeta );

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow( false );

    if ( !checkForUnresolved( oneMeta, BaseMessages.getString( PKG,
      "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs.PreviewTitle" ) ) ) {
      return;
    }

    TransMeta previewMeta =
        TransPreviewFactory.generatePreviewTransformation( transMeta, oneMeta, wStepname.getText() );

    EnterNumberDialog numberDialog =
        new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( PKG,
          "CassandraInputDialog.PreviewSize.DialogTitle" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "CassandraInputDialog.PreviewSize.DialogMessage" ) ); //$NON-NLS-1$

    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog =
          new TransPreviewProgressDialog( shell, previewMeta, new String[] {
            wStepname.getText() },
              new int[] {
                previewSize } );
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), //$NON-NLS-1$
                  BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), //$NON-NLS-1$
                  loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }
      PreviewRowsDialog prd =
          new PreviewRowsDialog( shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog
              .getPreviewRowsMeta( wStepname.getText() ),
              progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
      prd.open();
    }
  }

  protected void popupSchemaInfo() {

    DriverConnection conn = null;
    Keyspace kSpace = null;
    try {

      CassandraInputMeta cim = new CassandraInputMeta();
      cim.setDefault();
      getInfo( cim );

      String keyspaceS = transMeta.environmentSubstitute( cim.cassandraKeyspace );
      conn = CassandraInputData.getCassandraConnection( cim, transMeta, transMeta.getLogChannel() );
      kSpace = conn.getKeyspace( keyspaceS );

      try {

        IQueryMetaData metadata = kSpace.getQueryMetaData( transMeta.environmentSubstitute( cim.getCQLSelectQuery() ) );
        String tableName = metadata.getTableName();

        String schemaDescription = kSpace.getTableMetaData( tableName ).describe();
        ShowMessageDialog smd =
            new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK, "Schema info", schemaDescription,
                true ); // $NON-NLS-1$
        smd.open();
      } catch ( Exception e1 ) {
        logError(
          BaseMessages.getString( PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
              + ":\n\n" + e1.getMessage(), //$NON-NLS-1$
          e1 );
        new ErrorDialog( shell,
            BaseMessages.getString( PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
                + ":\n\n" + e1.getMessage(), //$NON-NLS-1$
            e1 );
      }

    } catch ( Exception e ) {
      logError(
        BaseMessages.getString( PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
            + ":\n\n" + e.getLocalizedMessage(), //$NON-NLS-1$
        e );
      new ErrorDialog( shell, BaseMessages.getString( PKG,
        "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
          BaseMessages.getString( PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
              + ":\n\n" + e.getLocalizedMessage(), //$NON-NLS-1$
          e );
      return;
    } finally {
      if ( conn != null ) {
        try {
          conn.closeConnection();
        } catch ( Exception e ) {
          log.logError( e.getLocalizedMessage(), e );
          // TODO popup another error dialog
        }
      }
    }
  }
}
