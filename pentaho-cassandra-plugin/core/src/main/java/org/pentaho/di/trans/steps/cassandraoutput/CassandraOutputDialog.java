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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.driver.datastax.DriverCQLRowHandler.ConsistencyLevelEnum;
import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.events.dialog.FilterType;
import org.pentaho.di.ui.core.events.dialog.ProviderFilterType;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog class for the CassandraOutput step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = CassandraOutputMeta.class;

  private final CassandraOutputMeta currentMeta;
  private final CassandraOutputMeta originalMeta;

  /**
   * various UI bits and pieces for the dialog
   */

  private CTabFolder wTabFolder;
  private CTabItem wConnectionTab;
  private CTabItem wWriteTab;
  private CTabItem wSchemaTab;

  private Label wlHost;
  private TextVar wHost;
  private Label wlPort;
  private TextVar wPort;

  private Label wlUser;
  private TextVar wUser;
  private Label wlPassword;
  private TextVar wPassword;

  private Button wSsl;

  private Label wlSocketTimeout;
  private TextVar wSocketTimeout;

  private Label wlKeyspace;
  private TextVar wKeyspace;

  private Label wlLocalDataCenter;
  private TextVar wLocalDataCenter;

  private Label wlTable;
  private CCombo wTable;
  private Button wbGetTables;

  private Label wlConsistency;
  private ComboVar wConsistency;

  private Label wlBatchSize;
  private TextVar wBatchSize;

  private Label wlBatchInsertTimeout;
  private TextVar wBatchInsertTimeout;
  private Label wlSubBatchSize;
  private TextVar wSubBatchSize;

  private Label wlUnloggedBatch;
  private Button wUnloggedBatch;

  private Label wlKeyField;
  private CCombo wKeyField;

  private Button wbGetFields;

  private Label wlSchemaHost;
  private TextVar wSchemaHost;
  private Label wlSchemaPort;
  private TextVar wSchemaPort;

  private Label wlCreateTable;
  private Button wCreateTable;

  private Label wlWithClause;
  private TextVar wWithClause;

  private Label wlTruncateTable;
  private Button wTruncateTable;

  private Label wlUpdateTableMetadata;
  private Button wUpdateTableMetadata;

  private Label wlInsertFieldsNotInTableMeta;
  private Button wInsertFieldsNotInTableMeta;

  private Label wlCompression;
  private Button wCompression;

  private Button wbShowSchema;

  private Button wbAprioriCql;

  private String aprioriCql;
  private boolean dontComplain;

  private CCombo wTtlUnitsCombo;
  private TextVar wTtlUnitsText;

  private TextVar wApplicationConf;
  private Button wbbApplicationConf;

  public CassandraOutputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );

    currentMeta = (CassandraOutputMeta) in;
    originalMeta = (CassandraOutputMeta) currentMeta.clone();
  }

  @Override
  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, currentMeta );

    // used to listen to a text field (m_wStepname)
    final ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        currentMeta.setChanged();
      }
    };

    changed = currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Shell.Title" ) ); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.StepName.Label" ) ); //$NON-NLS-1$
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    // start of the connection tab
    wConnectionTab = new CTabItem( wTabFolder, SWT.BORDER );
    wConnectionTab.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Tab.Connection" ) ); //$NON-NLS-1$

    Composite wConnectionComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wConnectionComp );

    FormLayout connectionLayout = new FormLayout();
    connectionLayout.marginWidth = 3;
    connectionLayout.marginHeight = 3;
    wConnectionComp.setLayout( connectionLayout );

    // Application conf
    Label wlApplicationConf = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlApplicationConf );
    wlApplicationConf.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.ApplicationConf.Label" ) );
    wlApplicationConf.setLayoutData( new FormDataBuilder().left( 0, 0 ).top( 0, margin ).right( middle, -margin ).result() );

    wbbApplicationConf = new Button( wConnectionComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbApplicationConf );
    wbbApplicationConf.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbApplicationConf.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    wbbApplicationConf.setLayoutData( new FormDataBuilder().top( 0, margin ).right( 100, 0 ).result() );

    wApplicationConf = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wApplicationConf );
    wApplicationConf.addModifyListener( lsMod );
    wApplicationConf.setLayoutData( new FormDataBuilder().left( middle, 0 ).top( 0, margin ).right( wbbApplicationConf, -margin ).result() );

    wbbApplicationConf.addSelectionListener( new SelectionAdapterFileDialogTextVar( log, wApplicationConf, transMeta,
      new SelectionAdapterOptions( SelectionOperation.FILE,
        new String[] {FilterType.CONF.toString(), FilterType.JSON.toString(), "properties",
          FilterType.ALL.toString()}, FilterType.CONF.toString(),
        new String[] {ProviderFilterType.LOCAL.toString()}, false ) ) );

    wApplicationConf.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent modifyEvent ) {
        updateApplicationConf();
      }
    } );

    // host line
    wlHost = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlHost );
    wlHost.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Hostname.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wbbApplicationConf, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlHost.setLayoutData( fd );

    wHost = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
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
    wlPort = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlPort );
    wlPort.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Port.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wHost, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fd );

    wPort = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
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


    wlLocalDataCenter = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlLocalDataCenter );
    wlLocalDataCenter.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.LocalDataCenter.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wPort, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlLocalDataCenter.setLayoutData( fd );

    wLocalDataCenter = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLocalDataCenter );
    wLocalDataCenter.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wLocalDataCenter.setToolTipText( transMeta.environmentSubstitute( wLocalDataCenter.getText() ) );
      }
    } );
    wLocalDataCenter.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wPort, margin );
    fd.left = new FormAttachment( middle, 0 );
    wLocalDataCenter.setLayoutData( fd );

    Label lSsl = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( lSsl );
    lSsl.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Ssl.Label" ) );
    lSsl.setLayoutData( new FormDataBuilder().left( 0, 0 ).top( wLocalDataCenter, margin ).right( middle, -margin ).result() );

    wSsl = new Button( wConnectionComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wSsl );
    wSsl.setLayoutData( new FormDataBuilder().left( middle, 0 ).top( wLocalDataCenter, margin ).right( 100, 0 ).result() );

    // socket timeout line
    wlSocketTimeout = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlSocketTimeout );
    wlSocketTimeout.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.SocketTimeout.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSsl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlSocketTimeout.setLayoutData( fd );

    wSocketTimeout = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSocketTimeout );
    wSocketTimeout.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wSocketTimeout.setToolTipText( transMeta.environmentSubstitute( wSocketTimeout.getText() ) );
      }
    } );
    wSocketTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSsl, margin );
    fd.left = new FormAttachment( middle, 0 );
    wSocketTimeout.setLayoutData( fd );

    // username line
    wlUser = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlUser );
    wlUser.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.User.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSocketTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlUser.setLayoutData( fd );

    wUser = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUser );
    wUser.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSocketTimeout, margin );
    fd.left = new FormAttachment( middle, 0 );
    wUser.setLayoutData( fd );

    // password line
    wlPassword = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlPassword );
    wlPassword.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Password.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wUser, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fd );

    wPassword = new PasswordTextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );

    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wUser, margin );
    fd.left = new FormAttachment( middle, 0 );
    wPassword.setLayoutData( fd );

    // keyspace line
    wlKeyspace = new Label( wConnectionComp, SWT.RIGHT );
    props.setLook( wlKeyspace );
    wlKeyspace.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Keyspace.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wPassword, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlKeyspace.setLayoutData( fd );

    wKeyspace = new TextVar( transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyspace );
    wKeyspace.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wKeyspace.setToolTipText( transMeta.environmentSubstitute( wKeyspace.getText() ) );
      }
    } );
    wKeyspace.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wPassword, margin );
    fd.left = new FormAttachment( middle, 0 );
    wKeyspace.setLayoutData( fd );


    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wConnectionComp.setLayoutData( fd );

    wConnectionComp.layout();
    wConnectionTab.setControl( wConnectionComp );

    // --- start of the write tab ---
    wWriteTab = new CTabItem( wTabFolder, SWT.NONE );
    wWriteTab.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Tab.Write" ) ); //$NON-NLS-1$
    Composite wWriteComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wWriteComp );

    FormLayout writeLayout = new FormLayout();
    writeLayout.marginWidth = 3;
    writeLayout.marginHeight = 3;
    wWriteComp.setLayout( writeLayout );

    // table line
    wlTable = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlTable );
    wlTable.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Table.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlTable.setLayoutData( fd );

    wbGetTables = new Button( wWriteComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetTables );
    wbGetTables.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.GetTable.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    wbGetTables.setLayoutData( fd );
    wbGetTables.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setupTablesCombo();
      }
    } );

    wTable = new CCombo( wWriteComp, SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wTable.setToolTipText( transMeta.environmentSubstitute( wTable.getText() ) );
      }
    } );
    wTable.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( wbGetTables, -margin );
    fd.top = new FormAttachment( 0, margin );
    fd.left = new FormAttachment( middle, 0 );
    wTable.setLayoutData( fd );

    // consistency line
    wlConsistency = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlConsistency );
    wlConsistency.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Consistency.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wTable, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlConsistency.setLayoutData( fd );
    wlConsistency.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.Consistency.Label.TipText" ) ); //$NON-NLS-1$

    wConsistency = new ComboVar( transMeta, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConsistency );
    wConsistency.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wConsistency.setToolTipText( transMeta.environmentSubstitute( wConsistency.getText() ) );
      }
    } );
    wConsistency.setItems( Arrays.stream( ConsistencyLevelEnum.values() ).map( cl -> cl.toString() ).collect( Collectors.toList() ).toArray( new String[0] ) );
    wConsistency.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wTable, margin );
    fd.left = new FormAttachment( middle, 0 );
    wConsistency.setLayoutData( fd );

    // batch size line
    wlBatchSize = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlBatchSize );
    wlBatchSize.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.BatchSize.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wConsistency, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlBatchSize.setLayoutData( fd );
    wlBatchSize.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.BatchSize.TipText" ) ); //$NON-NLS-1$

    wBatchSize = new TextVar( transMeta, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wBatchSize.setToolTipText( transMeta.environmentSubstitute( wBatchSize.getText() ) );
      }
    } );
    wBatchSize.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wConsistency, margin );
    fd.left = new FormAttachment( middle, 0 );
    wBatchSize.setLayoutData( fd );

    // batch insert timeout
    wlBatchInsertTimeout = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlBatchInsertTimeout );
    wlBatchInsertTimeout.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.BatchInsertTimeout.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wBatchSize, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlBatchInsertTimeout.setLayoutData( fd );
    wlBatchInsertTimeout.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.BatchInsertTimeout.TipText" ) ); //$NON-NLS-1$

    wBatchInsertTimeout = new TextVar( transMeta, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchInsertTimeout );
    wBatchInsertTimeout.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wBatchInsertTimeout.setToolTipText( transMeta.environmentSubstitute( wBatchInsertTimeout.getText() ) );
      }
    } );
    wBatchInsertTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wBatchSize, margin );
    fd.left = new FormAttachment( middle, 0 );
    wBatchInsertTimeout.setLayoutData( fd );

    // sub-batch size
    wlSubBatchSize = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlSubBatchSize );
    wlSubBatchSize.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.SubBatchSize.Label" ) ); //$NON-NLS-1$
    wlSubBatchSize.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.SubBatchSize.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wBatchInsertTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlSubBatchSize.setLayoutData( fd );

    wSubBatchSize = new TextVar( transMeta, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSubBatchSize );
    wSubBatchSize.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wSubBatchSize.setToolTipText( transMeta.environmentSubstitute( wSubBatchSize.getText() ) );
      }
    } );
    wSubBatchSize.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wBatchInsertTimeout, margin );
    fd.left = new FormAttachment( middle, 0 );
    wSubBatchSize.setLayoutData( fd );

    // unlogged batch line
    wlUnloggedBatch = new Label( wWriteComp, SWT.RIGHT );
    wlUnloggedBatch.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.UnloggedBatch.Label" ) ); //$NON-NLS-1$
    wlUnloggedBatch.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.UnloggedBatch.TipText" ) ); //$NON-NLS-1$
    props.setLook( wlUnloggedBatch );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSubBatchSize, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlUnloggedBatch.setLayoutData( fd );

    wUnloggedBatch = new Button( wWriteComp, SWT.CHECK );
    props.setLook( wUnloggedBatch );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSubBatchSize, margin );
    fd.left = new FormAttachment( middle, 0 );
    wUnloggedBatch.setLayoutData( fd );

    // TTL line
    Label ttlLab = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( ttlLab );
    ttlLab.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.TTL.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wUnloggedBatch, margin );
    fd.right = new FormAttachment( middle, -margin );
    ttlLab.setLayoutData( fd );

    wTtlUnitsCombo = new CCombo( wWriteComp, SWT.BORDER );
    wTtlUnitsCombo.setEditable( false );
    props.setLook( wTtlUnitsCombo );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wUnloggedBatch, margin );
    wTtlUnitsCombo.setLayoutData( fd );

    for ( CassandraOutputMeta.TTLUnits u : CassandraOutputMeta.TTLUnits.values() ) {
      wTtlUnitsCombo.add( u.toString() );
    }

    wTtlUnitsCombo.select( 0 );

    wTtlUnitsCombo.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        if ( wTtlUnitsCombo.getSelectionIndex() == 0 ) {
          wTtlUnitsText.setEnabled( false );
          wTtlUnitsText.setText( "" ); //$NON-NLS-1$
        } else {
          wTtlUnitsText.setEnabled( true );
        }
      }
    } );

    wTtlUnitsText = new TextVar( transMeta, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTtlUnitsText );
    fd = new FormData();
    fd.right = new FormAttachment( wTtlUnitsCombo, -margin );
    fd.top = new FormAttachment( wUnloggedBatch, margin );
    fd.left = new FormAttachment( middle, 0 );
    wTtlUnitsText.setLayoutData( fd );
    wTtlUnitsText.setEnabled( false );
    wTtlUnitsText.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wTtlUnitsText.setToolTipText( transMeta.environmentSubstitute( wTtlUnitsText.getText() ) );
      }
    } );

    // key field line
    wlKeyField = new Label( wWriteComp, SWT.RIGHT );
    props.setLook( wlKeyField );
    wlKeyField.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.KeyField.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wTtlUnitsText, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlKeyField.setLayoutData( fd );

    wbGetFields = new Button( wWriteComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetFields );
    wbGetFields.setText( " " //$NON-NLS-1$
      + BaseMessages.getString( PKG, "CassandraOutputDialog.GetFields.Button" ) //$NON-NLS-1$
      + " " ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wTtlUnitsText, margin );
    wbGetFields.setLayoutData( fd );

    wbGetFields.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        showEnterSelectionDialog();
      }
    } );

    wKeyField = new CCombo( wWriteComp, SWT.BORDER );
    wKeyField.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wKeyField.setToolTipText( transMeta.environmentSubstitute( wKeyField.getText() ) );
      }
    } );
    wKeyField.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( wbGetFields, -margin );
    fd.top = new FormAttachment( wTtlUnitsText, margin );
    fd.left = new FormAttachment( middle, 0 );
    wKeyField.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wWriteComp.setLayoutData( fd );

    wWriteComp.layout();
    wWriteTab.setControl( wWriteComp );

    // show schema button
    wbShowSchema = new Button( wWriteComp, SWT.PUSH );
    wbShowSchema.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.Schema.Button" ) ); //$NON-NLS-1$
    props.setLook( wbShowSchema );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    wbShowSchema.setLayoutData( fd );
    wbShowSchema.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        popupSchemaInfo();
      }
    } );

    // ---- start of the schema options tab ----
    wSchemaTab = new CTabItem( wTabFolder, SWT.NONE );
    wSchemaTab.setText( BaseMessages.getString( PKG, "CassandraOutputData.Tab.Schema" ) ); //$NON-NLS-1$

    Composite wSchemaComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSchemaComp );

    FormLayout schemaLayout = new FormLayout();
    schemaLayout.marginWidth = 3;
    schemaLayout.marginHeight = 3;
    wSchemaComp.setLayout( schemaLayout );

    // schema host line
    wlSchemaHost = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlSchemaHost );
    wlSchemaHost.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.SchemaHostname.Label" ) ); //$NON-NLS-1$
    wlSchemaHost.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.SchemaHostname.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlSchemaHost.setLayoutData( fd );

    wSchemaHost = new TextVar( transMeta, wSchemaComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaHost );
    wSchemaHost.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wSchemaHost.setToolTipText( transMeta.environmentSubstitute( wSchemaHost.getText() ) );
      }
    } );
    wSchemaHost.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.left = new FormAttachment( middle, 0 );
    wSchemaHost.setLayoutData( fd );

    // schema port line
    wlSchemaPort = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlSchemaPort );
    wlSchemaPort.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.SchemaPort.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSchemaHost, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlSchemaPort.setLayoutData( fd );

    wSchemaPort = new TextVar( transMeta, wSchemaComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaPort );
    wSchemaPort.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wSchemaPort.setToolTipText( transMeta.environmentSubstitute( wSchemaPort.getText() ) );
      }
    } );
    wSchemaPort.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSchemaHost, margin );
    fd.left = new FormAttachment( middle, 0 );
    wSchemaPort.setLayoutData( fd );

    // create table line
    wlCreateTable = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlCreateTable );
    wlCreateTable.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.CreateTable.Label" ) ); //$NON-NLS-1$
    wlCreateTable.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.CreateTable.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wSchemaPort, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlCreateTable.setLayoutData( fd );

    wCreateTable = new Button( wSchemaComp, SWT.CHECK );
    wCreateTable.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.CreateTable.TipText" ) ); //$NON-NLS-1$
    props.setLook( wCreateTable );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wSchemaPort, margin );
    fd.left = new FormAttachment( middle, 0 );
    wCreateTable.setLayoutData( fd );
    wCreateTable.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        currentMeta.setChanged();
      }
    } );

    // table creation with clause line
    wlWithClause = new Label( wSchemaComp, SWT.RIGHT );
    wlWithClause.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.CreateTableWithClause.Label" ) ); //$NON-NLS-1$
    wlWithClause
      .setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.CreateTableWithClause.TipText" ) ); //$NON-NLS-1$
    props.setLook( wlWithClause );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wCreateTable, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlWithClause.setLayoutData( fd );

    wWithClause = new TextVar( transMeta, wSchemaComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWithClause );
    wWithClause.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wWithClause.setToolTipText( transMeta.environmentSubstitute( wWithClause.getText() ) );
      }
    } );
    wWithClause.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wCreateTable, margin );
    fd.left = new FormAttachment( middle, 0 );
    wWithClause.setLayoutData( fd );

    // truncate table line
    wlTruncateTable = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlTruncateTable );
    wlTruncateTable
      .setText( BaseMessages.getString( PKG, "CassandraOutputDialog.TruncateTable.Label" ) ); //$NON-NLS-1$
    wlTruncateTable.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.TruncateTable.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wWithClause, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlTruncateTable.setLayoutData( fd );

    wTruncateTable = new Button( wSchemaComp, SWT.CHECK );
    wTruncateTable.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.TruncateTable.TipText" ) ); //$NON-NLS-1$
    props.setLook( wTruncateTable );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wWithClause, margin );
    fd.left = new FormAttachment( middle, 0 );
    wTruncateTable.setLayoutData( fd );
    wTruncateTable.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        currentMeta.setChanged();
      }
    } );

    // update table meta data line
    wlUpdateTableMetadata = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlUpdateTableMetadata );
    wlUpdateTableMetadata.setText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.UpdateTableMetaData.Label" ) ); //$NON-NLS-1$
    wlUpdateTableMetadata.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.UpdateTableMetaData.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wTruncateTable, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlUpdateTableMetadata.setLayoutData( fd );

    wUpdateTableMetadata = new Button( wSchemaComp, SWT.CHECK );
    wUpdateTableMetadata.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.UpdateTableMetaData.TipText" ) ); //$NON-NLS-1$
    props.setLook( wUpdateTableMetadata );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wTruncateTable, margin );
    fd.left = new FormAttachment( middle, 0 );
    wUpdateTableMetadata.setLayoutData( fd );
    wUpdateTableMetadata.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        currentMeta.setChanged();
      }
    } );

    // insert fields not in meta line
    wlInsertFieldsNotInTableMeta = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlInsertFieldsNotInTableMeta );
    wlInsertFieldsNotInTableMeta.setText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.InsertFieldsNotInTableMetaData.Label" ) ); //$NON-NLS-1$
    wlInsertFieldsNotInTableMeta.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.InsertFieldsNotInTableMetaData.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wUpdateTableMetadata, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlInsertFieldsNotInTableMeta.setLayoutData( fd );

    wInsertFieldsNotInTableMeta = new Button( wSchemaComp, SWT.CHECK );
    wInsertFieldsNotInTableMeta.setToolTipText( BaseMessages.getString( PKG,
      "CassandraOutputDialog.InsertFieldsNotInTableMetaData.TipText" ) ); //$NON-NLS-1$
    props.setLook( wInsertFieldsNotInTableMeta );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wUpdateTableMetadata, margin );
    fd.left = new FormAttachment( middle, 0 );
    wInsertFieldsNotInTableMeta.setLayoutData( fd );
    wInsertFieldsNotInTableMeta.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        currentMeta.setChanged();
      }
    } );

    // compression check box
    wlCompression = new Label( wSchemaComp, SWT.RIGHT );
    props.setLook( wlCompression );
    wlCompression.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.UseCompression.Label" ) ); //$NON-NLS-1$
    wlCompression.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.UseCompression.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wInsertFieldsNotInTableMeta, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlCompression.setLayoutData( fd );

    wCompression = new Button( wSchemaComp, SWT.CHECK );
    props.setLook( wCompression );
    wCompression.setToolTipText( BaseMessages.getString( PKG, "CassandraOutputDialog.UseCompression.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( wInsertFieldsNotInTableMeta, margin );
    wCompression.setLayoutData( fd );
    wCompression.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        currentMeta.setChanged();
      }
    } );

    // Apriori CQL button
    wbAprioriCql = new Button( wSchemaComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbAprioriCql );
    wbAprioriCql.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.CQL.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    wbAprioriCql.setLayoutData( fd );
    wbAprioriCql.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        popupCQLEditor( lsMod );
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wSchemaComp.setLayoutData( fd );

    wSchemaComp.layout();
    wSchemaTab.setControl( wSchemaComp );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wStepname, margin );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fd );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    setButtonPositions( new Button[] {wOK, wCancel}, margin, wTabFolder );

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

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

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

    wTabFolder.setSelection( 0 );
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
    boolean usingConf = !Utils.isEmpty( wApplicationConf.getText() );
    wSsl.setEnabled( !usingConf );
    wSocketTimeout.setEnabled( !usingConf );
  }

  protected void setupTablesCombo() {
    DriverConnection conn = null;
    Keyspace kSpace = null;

    try {
      String hostS = transMeta.environmentSubstitute( wHost.getText() );
      String portS = transMeta.environmentSubstitute( wPort.getText() );
      String userS = wUser.getText();
      String passS = wPassword.getText();
      if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
        userS = transMeta.environmentSubstitute( userS );
        passS = transMeta.environmentSubstitute( passS );
      }
      String keyspaceS = transMeta.environmentSubstitute( wKeyspace.getText() );
      String localDataCenter = transMeta.environmentSubstitute( wLocalDataCenter.getText() );
      String applicationConf = transMeta.environmentSubstitute( wApplicationConf.getText() );

      try {
        Map<String, String> opts = new HashMap<String, String>();
        if ( Utils.isEmpty( applicationConf ) ) {
          if ( wSsl.getSelection() ) {
            opts.put( CassandraUtils.ConnectionOptions.SSL, "Y" );
          }
        } else {
          opts.put( CassandraUtils.ConnectionOptions.APPLICATION_CONF, applicationConf );
        }
        opts.put( CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING );
        conn =
          CassandraUtils.getCassandraConnection( hostS, Const.toInt( portS, -1 ), userS, passS,
            ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL, opts, localDataCenter );

        kSpace = conn.getKeyspace( keyspaceS );
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getLocalizedMessage(), e ); //$NON-NLS-1$
        new ErrorDialog( shell, BaseMessages.getString( PKG,
          "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
          BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
            + ":\n\n" + e.getLocalizedMessage(), e ); //$NON-NLS-1$
        return;
      }

      List<String> tables = kSpace.getTableNamesCQL3();
      wTable.removeAll();
      for ( String famName : tables ) {
        wTable.add( famName );
      }

    } catch ( Exception ex ) {
      logError( BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
        + ":\n\n" + ex.getMessage(), ex ); //$NON-NLS-1$
      new ErrorDialog( shell, BaseMessages
        .getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
        BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
          + ":\n\n" + ex.getMessage(), ex ); //$NON-NLS-1$
    } finally {
      if ( conn != null ) {
        try {
          conn.closeConnection();
        } catch ( Exception e ) {
          // TODO popup another error dialog
          e.printStackTrace();
        }
      }
    }
  }

  protected void showEnterSelectionDialog() {
    StepMeta stepMeta = transMeta.findStep( stepname );

    String[] choices = null;
    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

        if ( row.size() == 0 ) {
          MessageDialog.openError( shell, BaseMessages.getString( PKG,
            "CassandraOutputData.Message.NoIncomingFields.Title" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "CassandraOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$

          return;
        }

        choices = new String[row.size()];
        for ( int i = 0; i < row.size(); i++ ) {
          ValueMetaInterface vm = row.getValueMeta( i );
          choices[i] = vm.getName();
        }

        EnterSelectionDialog dialog =
          new EnterSelectionDialog( shell, choices, BaseMessages.getString( PKG,
            "CassandraOutputDialog.SelectKeyFieldsDialog.Title" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Message" ) ); //$NON-NLS-1$
        dialog.setMulti( true );
        if ( !Utils.isEmpty( wKeyField.getText() ) ) {
          String current = wKeyField.getText();
          String[] parts = current.split( "," ); //$NON-NLS-1$
          int[] currentSelection = new int[parts.length];
          int count = 0;
          for ( String s : parts ) {
            int index = row.indexOfValue( s.trim() );
            if ( index >= 0 ) {
              currentSelection[count++] = index;
            }
          }

          dialog.setSelectedNrs( currentSelection );
        }

        dialog.open();

        int[] selected = dialog.getSelectionIndeces(); // SIC
        if ( selected != null && selected.length > 0 ) {
          StringBuilder newSelection = new StringBuilder();
          boolean first = true;
          for ( int i : selected ) {
            if ( first ) {
              newSelection.append( choices[i] );
              first = false;
            } else {
              newSelection.append( "," ).append( choices[i] ); //$NON-NLS-1$
            }
          }

          wKeyField.setText( newSelection.toString() );
        }
      } catch ( KettleException ex ) {
        MessageDialog.openError( shell, BaseMessages.getString( PKG,
          "CassandraOutputData.Message.NoIncomingFields.Title" ), BaseMessages //$NON-NLS-1$
          .getString( PKG, "CassandraOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$
      }
    }
  }

  protected void setupFieldsCombo() {
    // try and set up from incoming fields from previous step

    StepMeta stepMeta = transMeta.findStep( stepname );

    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

        if ( row.size() == 0 ) {
          MessageDialog.openError( shell, BaseMessages.getString( PKG,
            "CassandraOutputData.Message.NoIncomingFields.Title" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "CassandraOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$

          return;
        }

        wKeyField.removeAll();
        for ( int i = 0; i < row.size(); i++ ) {
          ValueMetaInterface vm = row.getValueMeta( i );
          wKeyField.add( vm.getName() );
        }
      } catch ( KettleException ex ) {
        MessageDialog.openError( shell, BaseMessages.getString( PKG,
          "CassandraOutputData.Message.NoIncomingFields.Title" ), BaseMessages //$NON-NLS-1$
          .getString( PKG, "CassandraOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$
      }
    }
  }

  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText();
    currentMeta.setCassandraHost( wHost.getText() );
    currentMeta.setCassandraPort( wPort.getText() );
    currentMeta.setSchemaHost( wSchemaHost.getText() );
    currentMeta.setSchemaPort( wSchemaPort.getText() );
    currentMeta.setSocketTimeout( wSocketTimeout.getText() );
    currentMeta.setUsername( wUser.getText() );
    currentMeta.setPassword( wPassword.getText() );
    currentMeta.setCassandraKeyspace( wKeyspace.getText() );
    currentMeta.setLocalDataCenter( wLocalDataCenter.getText() );
    currentMeta.setTableName( wTable.getText() );
    currentMeta.setConsistency( wConsistency.getText() );
    currentMeta.setBatchSize( wBatchSize.getText() );
    currentMeta.setCQLBatchInsertTimeout( wBatchInsertTimeout.getText() );
    currentMeta.setCQLSubBatchSize( wSubBatchSize.getText() );
    currentMeta.setKeyField( wKeyField.getText() );

    currentMeta.setCreateTable( wCreateTable.getSelection() );
    currentMeta.setTruncateTable( wTruncateTable.getSelection() );
    currentMeta.setUpdateCassandraMeta( wUpdateTableMetadata.getSelection() );
    currentMeta.setInsertFieldsNotInMeta( wInsertFieldsNotInTableMeta.getSelection() );
    currentMeta.setUseCompression( wCompression.getSelection() );
    currentMeta.setAprioriCQL( aprioriCql );
    currentMeta.setCreateTableClause( wWithClause.getText() );
    currentMeta.setDontComplainAboutAprioriCQLFailing( dontComplain );
    currentMeta.setUseUnloggedBatches( wUnloggedBatch.getSelection() );
    currentMeta.setSsl( wSsl.getSelection() );
    currentMeta.setApplicationConf( wApplicationConf.getText() );

    currentMeta.setTTL( wTtlUnitsText.getText() );
    currentMeta.setTTLUnit( wTtlUnitsCombo.getText() );

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

  protected void popupCQLEditor( ModifyListener lsMod ) {

    EnterCQLDialog ecd =
      new EnterCQLDialog( shell, transMeta, lsMod, BaseMessages.getString( PKG, "CassandraOutputDialog.CQL.Button" ), //$NON-NLS-1$
        aprioriCql, dontComplain );

    aprioriCql = ecd.open();
    dontComplain = ecd.getDontComplainStatus();
  }

  protected void popupSchemaInfo() {

    DriverConnection conn = null;
    Keyspace kSpace = null;
    try {
      String hostS = transMeta.environmentSubstitute( wHost.getText() );
      String portS = transMeta.environmentSubstitute( wPort.getText() );
      String userS = wUser.getText();
      String passS = wPassword.getText();
      if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
        userS = transMeta.environmentSubstitute( userS );
        passS = transMeta.environmentSubstitute( passS );
      }
      String keyspaceS = transMeta.environmentSubstitute( wKeyspace.getText() );
      String localDataCenter = transMeta.environmentSubstitute( wLocalDataCenter.getText() );
      String applicationConf = transMeta.environmentSubstitute( wApplicationConf.getText() );

      try {
        Map<String, String> opts = new HashMap<String, String>();
        opts.put( CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING );
        if ( Utils.isEmpty( applicationConf ) ) {
          if ( wSsl.getSelection() ) {
            opts.put( CassandraUtils.ConnectionOptions.SSL, "Y" );
          }
        } else {
          opts.put( CassandraUtils.ConnectionOptions.APPLICATION_CONF, applicationConf );
        }

        conn = CassandraUtils.getCassandraConnection( hostS, Const.toInt( portS, -1 ), userS, passS,
          ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
          opts, localDataCenter );

        kSpace = conn.getKeyspace( keyspaceS );

      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getLocalizedMessage(), e ); //$NON-NLS-1$
        new ErrorDialog( shell, BaseMessages.getString( PKG,
          "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
          BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
            + ":\n\n" + e.getLocalizedMessage(), e ); //$NON-NLS-1$
        return;
      }

      String table = transMeta.environmentSubstitute( wTable.getText() );
      if ( Utils.isEmpty( table ) ) {
        throw new Exception( "No table name specified!" ); //$NON-NLS-1$
      }
      table = CassandraUtils.cql3MixedCaseQuote( table );

      // if (!CassandraColumnMetaData.tableExists(conn, table)) {
      if ( !kSpace.tableExists( table ) ) {
        throw new Exception( "The table '" + table + "' does not " //$NON-NLS-1$ //$NON-NLS-2$
          + "seem to exist in the keyspace '" + keyspaceS ); //$NON-NLS-1$
      }

      ITableMetaData cassMeta = kSpace.getTableMetaData( table );
      // CassandraColumnMetaData cassMeta = new CassandraColumnMetaData(conn,
      // table);
      String schemaDescription = cassMeta.describe();
      ShowMessageDialog smd =
        new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK, "Schema info", schemaDescription, true ); //$NON-NLS-1$
      smd.open();
    } catch ( Exception e1 ) {
      logError( BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
        + ":\n\n" + e1.getMessage(), e1 ); //$NON-NLS-1$
      new ErrorDialog( shell, BaseMessages
        .getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title" ), //$NON-NLS-1$
        BaseMessages.getString( PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message" ) //$NON-NLS-1$
          + ":\n\n" + e1.getMessage(), e1 ); //$NON-NLS-1$
    } finally {
      if ( conn != null ) {
        try {
          conn.closeConnection();
        } catch ( Exception e ) {
          // TODO popup another error dialog
          e.printStackTrace();
        }
      }
    }
  }

  protected void getData() {

    wHost.setText( Const.nullToEmpty( currentMeta.getCassandraHost() ) );

    wPort.setText( Const.nullToEmpty( currentMeta.getCassandraPort() ) );

    wSchemaHost.setText( Const.nullToEmpty( currentMeta.getSchemaHost() ) );

    wSchemaPort.setText( Const.nullToEmpty( currentMeta.getSchemaPort() ) );

    wSocketTimeout.setText( Const.nullToEmpty( currentMeta.getSocketTimeout() ) );

    wUser.setText( Const.nullToEmpty( currentMeta.getUsername() ) );

    wPassword.setText( Const.nullToEmpty( currentMeta.getPassword() ) );

    wKeyspace.setText( Const.nullToEmpty( currentMeta.getCassandraKeyspace() ) );

    wLocalDataCenter.setText( Const.NVL( currentMeta.getLocalDataCenter(), "" ) );

    wTable.setText( Const.nullToEmpty( currentMeta.getTableName() ) );

    wConsistency.setText( Const.nullToEmpty( currentMeta.getConsistency() ) );

    wBatchSize.setText( Const.nullToEmpty( currentMeta.getBatchSize() ) );

    wBatchInsertTimeout.setText( Const.nullToEmpty( currentMeta.getCQLBatchInsertTimeout() ) );

    wSubBatchSize.setText( Const.nullToEmpty( currentMeta.getCQLSubBatchSize() ) );

    wKeyField.setText( Const.nullToEmpty( currentMeta.getKeyField() ) );

    wWithClause.setText( Const.nullToEmpty( currentMeta.getCreateTableWithClause() ) );

    wApplicationConf.setText( Const.nullToEmpty( currentMeta.getApplicationConf() ) );

    wCreateTable.setSelection( currentMeta.getCreateTable() );
    wTruncateTable.setSelection( currentMeta.getTruncateTable() );
    wUpdateTableMetadata.setSelection( currentMeta.getUpdateCassandraMeta() );
    wInsertFieldsNotInTableMeta.setSelection( currentMeta.getInsertFieldsNotInMeta() );
    wCompression.setSelection( currentMeta.getUseCompression() );
    wUnloggedBatch.setSelection( currentMeta.getUseUnloggedBatch() );

    dontComplain = currentMeta.getDontComplainAboutAprioriCQLFailing();

    aprioriCql = Const.nullToEmpty( currentMeta.getAprioriCQL() );

    wbGetFields.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.SelectFields.Button" ) ); //$NON-NLS-1$
    wlKeyField.setText( BaseMessages.getString( PKG, "CassandraOutputDialog.KeyFields.Label" ) ); //$NON-NLS-1$

    if ( !Utils.isEmpty( currentMeta.getTTL() ) ) {
      wTtlUnitsText.setText( currentMeta.getTTL() );
      wTtlUnitsCombo.setText( currentMeta.getTTLUnit() );
      wTtlUnitsText.setEnabled( wTtlUnitsCombo.getSelectionIndex() > 0 );
    }
    wSsl.setSelection( currentMeta.isSsl() );
  }
}
