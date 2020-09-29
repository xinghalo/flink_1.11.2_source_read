import { ChangeDetectorRef } from '@angular/core';
import { NzMessageComponent } from '../message/nz-message.component';
import { NzNotificationContainerComponent } from './nz-notification-container.component';
import { NzNotificationDataFilled } from './nz-notification.definitions';
export declare class NzNotificationComponent extends NzMessageComponent {
    private container;
    protected cdr: ChangeDetectorRef;
    nzMessage: NzNotificationDataFilled;
    constructor(container: NzNotificationContainerComponent, cdr: ChangeDetectorRef);
    close(): void;
    readonly state: string | undefined;
}
