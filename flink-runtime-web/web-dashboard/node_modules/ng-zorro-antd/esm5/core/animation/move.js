/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationDuration } from './animation';
/** @type {?} */
export var moveUpMotion = trigger('moveUpMotion', [
    transition('* => enter', [
        style({
            transformOrigin: '0 0',
            transform: 'translateY(-100%)',
            opacity: 0
        }),
        animate("" + AnimationDuration.BASE, style({
            transformOrigin: '0 0',
            transform: 'translateY(0%)',
            opacity: 1
        }))
    ]),
    transition('* => leave', [
        style({
            transformOrigin: '0 0',
            transform: 'translateY(0%)',
            opacity: 1
        }),
        animate("" + AnimationDuration.BASE, style({
            transformOrigin: '0 0',
            transform: 'translateY(-100%)',
            opacity: 0
        }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW92ZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9tb3ZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sYUFBYSxDQUFDOztBQUVoRCxNQUFNLEtBQU8sWUFBWSxHQUE2QixPQUFPLENBQUMsY0FBYyxFQUFFO0lBQzVFLFVBQVUsQ0FBQyxZQUFZLEVBQUU7UUFDdkIsS0FBSyxDQUFDO1lBQ0osZUFBZSxFQUFFLEtBQUs7WUFDdEIsU0FBUyxFQUFRLG1CQUFtQjtZQUNwQyxPQUFPLEVBQVUsQ0FBQztTQUNuQixDQUFDO1FBQ0YsT0FBTyxDQUFDLEtBQUcsaUJBQWlCLENBQUMsSUFBTSxFQUFFLEtBQUssQ0FBQztZQUN6QyxlQUFlLEVBQUUsS0FBSztZQUN0QixTQUFTLEVBQVEsZ0JBQWdCO1lBQ2pDLE9BQU8sRUFBVSxDQUFDO1NBQ25CLENBQUMsQ0FBQztLQUNKLENBQUM7SUFDRixVQUFVLENBQUMsWUFBWSxFQUFFO1FBQ3ZCLEtBQUssQ0FBQztZQUNKLGVBQWUsRUFBRSxLQUFLO1lBQ3RCLFNBQVMsRUFBUSxnQkFBZ0I7WUFDakMsT0FBTyxFQUFVLENBQUM7U0FDbkIsQ0FBQztRQUNGLE9BQU8sQ0FBQyxLQUFHLGlCQUFpQixDQUFDLElBQU0sRUFBRSxLQUFLLENBQUM7WUFDekMsZUFBZSxFQUFFLEtBQUs7WUFDdEIsU0FBUyxFQUFRLG1CQUFtQjtZQUNwQyxPQUFPLEVBQVUsQ0FBQztTQUNuQixDQUFDLENBQUM7S0FDSixDQUFDO0NBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIGFuaW1hdGUsXG4gIHN0eWxlLFxuICB0cmFuc2l0aW9uLFxuICB0cmlnZ2VyLFxuICBBbmltYXRpb25UcmlnZ2VyTWV0YWRhdGFcbn0gZnJvbSAnQGFuZ3VsYXIvYW5pbWF0aW9ucyc7XG5pbXBvcnQgeyBBbmltYXRpb25EdXJhdGlvbiB9IGZyb20gJy4vYW5pbWF0aW9uJztcblxuZXhwb3J0IGNvbnN0IG1vdmVVcE1vdGlvbjogQW5pbWF0aW9uVHJpZ2dlck1ldGFkYXRhID0gdHJpZ2dlcignbW92ZVVwTW90aW9uJywgW1xuICB0cmFuc2l0aW9uKCcqID0+IGVudGVyJywgW1xuICAgIHN0eWxlKHtcbiAgICAgIHRyYW5zZm9ybU9yaWdpbjogJzAgMCcsXG4gICAgICB0cmFuc2Zvcm0gICAgICA6ICd0cmFuc2xhdGVZKC0xMDAlKScsXG4gICAgICBvcGFjaXR5ICAgICAgICA6IDBcbiAgICB9KSxcbiAgICBhbmltYXRlKGAke0FuaW1hdGlvbkR1cmF0aW9uLkJBU0V9YCwgc3R5bGUoe1xuICAgICAgdHJhbnNmb3JtT3JpZ2luOiAnMCAwJyxcbiAgICAgIHRyYW5zZm9ybSAgICAgIDogJ3RyYW5zbGF0ZVkoMCUpJyxcbiAgICAgIG9wYWNpdHkgICAgICAgIDogMVxuICAgIH0pKVxuICBdKSxcbiAgdHJhbnNpdGlvbignKiA9PiBsZWF2ZScsIFtcbiAgICBzdHlsZSh7XG4gICAgICB0cmFuc2Zvcm1PcmlnaW46ICcwIDAnLFxuICAgICAgdHJhbnNmb3JtICAgICAgOiAndHJhbnNsYXRlWSgwJSknLFxuICAgICAgb3BhY2l0eSAgICAgICAgOiAxXG4gICAgfSksXG4gICAgYW5pbWF0ZShgJHtBbmltYXRpb25EdXJhdGlvbi5CQVNFfWAsIHN0eWxlKHtcbiAgICAgIHRyYW5zZm9ybU9yaWdpbjogJzAgMCcsXG4gICAgICB0cmFuc2Zvcm0gICAgICA6ICd0cmFuc2xhdGVZKC0xMDAlKScsXG4gICAgICBvcGFjaXR5ICAgICAgICA6IDBcbiAgICB9KSlcbiAgXSlcbl0pO1xuIl19