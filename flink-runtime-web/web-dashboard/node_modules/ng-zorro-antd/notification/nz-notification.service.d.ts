import { Overlay } from '@angular/cdk/overlay';
import { ApplicationRef, ComponentFactoryResolver, Injector, TemplateRef } from '@angular/core';
import { NzMessageBaseService } from '../message/nz-message.service';
import { NzNotificationConfig } from './nz-notification-config';
import { NzNotificationContainerComponent } from './nz-notification-container.component';
import { NzNotificationData, NzNotificationDataFilled, NzNotificationDataOptions } from './nz-notification.definitions';
export declare class NzNotificationService extends NzMessageBaseService<NzNotificationContainerComponent, NzNotificationData, NzNotificationConfig> {
    constructor(overlay: Overlay, injector: Injector, cfr: ComponentFactoryResolver, appRef: ApplicationRef);
    success(title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    error(title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    info(title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    warning(title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    blank(title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    create(type: 'success' | 'info' | 'warning' | 'error' | 'blank' | string, title: string, content: string, options?: NzNotificationDataOptions): NzNotificationDataFilled;
    template(template: TemplateRef<{}>, options?: NzNotificationDataOptions): NzNotificationDataFilled;
}
